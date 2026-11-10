import pandas as pd
import h3
import duckdb
from pathlib import Path
from .common import SECTORS
from .normalize import DATA_TYPES

MODELS_SQL = (Path(__file__).parent / "sql/models.sql").read_text(encoding="utf-8")


def open_warehouse(path, read_only=False):
    """Use UTC for both interactive queries and the batch writer."""
    con = duckdb.connect(str(path), read_only=read_only)
    con.execute("SET TimeZone='UTC'")
    con.execute("SET threads=2")
    con.execute("SET memory_limit='1GB'")
    return con


def initialize_db(con):
    definitions = ", ".join(f'"{name}" {kind}' for name, kind in DATA_TYPES.items())
    con.execute(f"CREATE TABLE IF NOT EXISTS fact_observations ({definitions}, PRIMARY KEY(observation_id))")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS observation_versions ({definitions}, first_seen_run VARCHAR, first_seen_at TIMESTAMPTZ, PRIMARY KEY(observation_id, record_hash))"
    )
    con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id VARCHAR PRIMARY KEY, mode VARCHAR, started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ, status VARCHAR, metrics_json VARCHAR, error VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS quality_results (
        run_id VARCHAR, check_name VARCHAR, severity VARCHAR, failures BIGINT,
        PRIMARY KEY(run_id,check_name))""")
    con.execute("""CREATE TABLE IF NOT EXISTS retirements (
        run_id VARCHAR, observation_id VARCHAR, retired_at TIMESTAMPTZ,
        reason VARCHAR, PRIMARY KEY(run_id, observation_id))""")
    con.execute("""CREATE TABLE IF NOT EXISTS observation_state (
        observation_id VARCHAR PRIMARY KEY, last_seen_cutoff TIMESTAMPTZ,
        active BOOLEAN, latest_hash VARCHAR, latest_modified TIMESTAMPTZ)""")
    con.execute("""CREATE TABLE IF NOT EXISTS checkpoint (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), watermark_utc TIMESTAMPTZ,
        last_full_utc TIMESTAMPTZ, applied_cutoff TIMESTAMPTZ, last_successful_run VARCHAR)""")
    con.execute("INSERT INTO checkpoint(singleton) VALUES (1) ON CONFLICT DO NOTHING")


def checkpoint(con):
    from .common import iso

    row = con.execute(
        "SELECT watermark_utc, last_full_utc, applied_cutoff, last_successful_run FROM checkpoint"
    ).fetchone()
    return {
        k: iso(v) if hasattr(v, "isoformat") else v
        for k, v in zip(["watermark_utc", "last_full_utc", "applied_cutoff", "last_successful_run"], row)
    }


def load_current(con, parquet_path, run_id, cutoff, full_refresh=False, confirmed_absent=()):
    """Caller owns the transaction. Archive cutoff orders equal-time content changes."""
    casts = ", ".join(f'CAST("{name}" AS {kind}) AS "{name}"' for name, kind in DATA_TYPES.items())
    con.execute(f"CREATE OR REPLACE TEMP TABLE incoming AS SELECT {casts} FROM read_parquet(?)", [str(parquet_path)])
    incoming_count = con.execute("SELECT count(*) FROM incoming").fetchone()[0]
    conflict = con.execute(
        """SELECT count(*) FROM incoming i JOIN observation_state s USING(observation_id)
        WHERE s.active AND i.modified_at=s.latest_modified AND i.record_hash<>s.latest_hash
        AND s.last_seen_cutoff=CAST(? AS TIMESTAMPTZ)""",
        [cutoff],
    ).fetchone()[0]
    if conflict:
        raise ValueError("Conflicting revisions with equal modification and extraction timestamps")
    con.execute(
        """CREATE OR REPLACE TEMP TABLE changes AS
        SELECT i.* FROM incoming i LEFT JOIN observation_state s USING(observation_id)
        WHERE s.observation_id IS NULL OR
          (CAST(? AS TIMESTAMPTZ) > s.last_seen_cutoff AND NOT s.active AND i.modified_at>=s.latest_modified) OR
          (s.active AND (i.modified_at>s.latest_modified OR
            (i.modified_at=s.latest_modified AND CAST(? AS TIMESTAMPTZ)>s.last_seen_cutoff))
            AND i.record_hash<>s.latest_hash)""",
        [cutoff, cutoff],
    )
    new = con.execute("""SELECT count(*) FROM changes c LEFT JOIN observation_state s USING(observation_id)
        WHERE s.observation_id IS NULL""").fetchone()[0]
    restored = con.execute("""SELECT count(*) FROM changes c JOIN observation_state s USING(observation_id)
        WHERE NOT s.active""").fetchone()[0]
    count = con.execute("SELECT count(*) FROM changes").fetchone()[0]
    con.execute(
        """INSERT INTO observation_versions
        SELECT i.*, ?, now() FROM incoming i WHERE NOT EXISTS (
          SELECT 1 FROM observation_versions v WHERE v.observation_id=i.observation_id AND v.record_hash=i.record_hash)""",
        [run_id],
    )
    con.execute("DELETE FROM fact_observations WHERE observation_id IN (SELECT observation_id FROM changes)")
    con.execute("INSERT INTO fact_observations SELECT * FROM changes")
    con.execute(
        """INSERT INTO observation_state
        SELECT observation_id, CAST(? AS TIMESTAMPTZ), TRUE, record_hash, modified_at FROM changes
        ON CONFLICT(observation_id) DO UPDATE SET last_seen_cutoff=excluded.last_seen_cutoff,
            active=TRUE, latest_hash=excluded.latest_hash, latest_modified=excluded.latest_modified""",
        [cutoff],
    )
    con.execute(
        """UPDATE observation_state s SET last_seen_cutoff=greatest(s.last_seen_cutoff, CAST(? AS TIMESTAMPTZ))
        FROM incoming i WHERE s.observation_id=i.observation_id AND s.active AND s.latest_hash=i.record_hash""",
        [cutoff],
    )
    removed = 0
    if full_refresh and confirmed_absent:
        absent = pd.DataFrame({"observation_id": list(confirmed_absent)})
        con.register("absent", absent)
        con.execute(
            """CREATE OR REPLACE TEMP TABLE retiring AS SELECT f.observation_id FROM fact_observations f
            JOIN absent a USING(observation_id) JOIN observation_state s USING(observation_id)
            WHERE s.last_seen_cutoff < CAST(? AS TIMESTAMPTZ)""",
            [cutoff],
        )
        removed = con.execute("SELECT count(*) FROM retiring").fetchone()[0]
        con.execute(
            "INSERT INTO retirements SELECT ?, observation_id, CAST(? AS TIMESTAMPTZ), 'absent_from_published_scope' FROM retiring",
            [run_id, cutoff],
        )
        con.execute("DELETE FROM fact_observations WHERE observation_id IN (SELECT observation_id FROM retiring)")
        con.execute(
            """UPDATE observation_state SET active=FALSE, last_seen_cutoff=CAST(? AS TIMESTAMPTZ)
            WHERE observation_id IN (SELECT observation_id FROM retiring)""",
            [cutoff],
        )
        con.unregister("absent")
    return {
        "new": new,
        "updated": count - new - restored,
        "restored": restored,
        "unchanged_or_older": incoming_count - count,
        "removed": removed,
    }


def build_models(con):
    lookup = pd.DataFrame(SECTORS.items(), columns=["sector_code", "sector_name"])
    con.register("sector_lookup", lookup)
    con.execute(MODELS_SQL)
    con.unregister("sector_lookup")
    cells = con.execute("SELECT h3_cell FROM mart_locations ORDER BY h3_cell").fetchall()
    centers = [(c[0], *h3.cell_to_latlng(c[0])) for c in cells]
    centers = pd.DataFrame(centers, columns=["h3_cell", "center_latitude", "center_longitude"])
    con.register("centers", centers)
    con.execute(
        "CREATE OR REPLACE TABLE dim_location AS SELECT CAST(h3_cell AS VARCHAR) AS h3_cell, CAST(center_latitude AS DOUBLE) AS center_latitude, CAST(center_longitude AS DOUBLE) AS center_longitude FROM centers"
    )
    con.unregister("centers")


class WarehouseQualityError(ValueError):
    def __init__(self, checks):
        self.checks = checks
        super().__init__("Warehouse reconciliation failed")


def warehouse_checks(con):
    checks = {
        "active_state_matches_facts": "SELECT count(*) FROM observation_state s FULL JOIN fact_observations f USING(observation_id) WHERE (s.active AND f.observation_id IS NULL) OR (NOT s.active AND f.observation_id IS NOT NULL) OR s.observation_id IS NULL",
        "fact_has_history": "SELECT count(*) FROM fact_observations f LEFT JOIN observation_versions v USING(observation_id,record_hash) WHERE v.observation_id IS NULL",
        "hidden_estimates_masked": "SELECT count(*) FROM fact_observations WHERE hide_emission AND (emission_rate_kg_hr IS NOT NULL OR uncertainty_kg_hr IS NOT NULL)",
        "location_dimension_complete": "SELECT count(*) FROM fact_observations f LEFT JOIN dim_location d USING(h3_cell) WHERE d.h3_cell IS NULL",
        "monthly_counts_reconcile": "SELECT abs((SELECT count(*) FROM fact_observations)-(SELECT coalesce(sum(observations),0) FROM mart_monthly))",
        "location_counts_reconcile": "SELECT abs((SELECT count(*) FROM fact_observations)-(SELECT coalesce(sum(observations),0) FROM mart_locations))",
    }
    result = [
        {"check": name, "severity": "error", "failures": con.execute(sql).fetchone()[0]} for name, sql in checks.items()
    ]
    if any(row["failures"] for row in result):
        raise WarehouseQualityError(result)
    return result
