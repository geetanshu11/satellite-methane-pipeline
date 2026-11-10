import json
import dataclasses
import subprocess
import sys
from pathlib import Path
import duckdb
import pytest
from methane.api import extract
from methane.common import project_lock
from methane.config import load_config
from methane.pipeline import run_pipeline, replay_archives
from methane.warehouse import checkpoint, open_warehouse
from conftest import observation, Session, page

ROOT = Path(__file__).parents[1]
pytestmark = pytest.mark.integration


def ingest(cfg, items, mode="full", confirm=None):
    responses = [page(items)]
    if confirm is not None:
        responses.append(page(confirm))
    return run_pipeline(cfg, mode, session=Session(responses))


def facts(cfg):
    with duckdb.connect(str(cfg.db_path), read_only=True) as c:
        return c.execute("SELECT observation_id, record_hash FROM fact_observations ORDER BY 1").fetchall()


def test_idempotency_and_revisions(cfg):
    ingest(cfg, [observation()])
    before = facts(cfg)
    second = ingest(cfg, [observation()], "incremental")
    assert facts(cfg) == before and second["metrics"]["new"] == second["metrics"]["updated"] == 0
    third = ingest(cfg, [observation(emission_auto=200, modified="2025-02-02T00:00:00Z")], "incremental")
    assert third["metrics"]["updated"] == 1 and third["metrics"]["history_versions"] == 2
    fourth = ingest(cfg, [observation()], "incremental")
    assert fourth["metrics"]["updated"] == 0 and fourth["metrics"]["history_versions"] == 2


def test_equal_time_revision_uses_later_capture(cfg):
    ingest(cfg, [observation()])
    assert ingest(cfg, [observation(emission_auto=101)], "incremental")["metrics"]["updated"] == 1


def test_full_withdrawal_and_republication(cfg):
    ingest(cfg, [observation(), observation("b")])
    assert ingest(cfg, [], "incremental")["metrics"]["current_observations"] == 2
    full = ingest(cfg, [observation()], confirm=[])
    assert full["metrics"]["removed"] == 1 and full["metrics"]["history_versions"] == 2
    restored = ingest(cfg, [observation("b")], "incremental")
    assert restored["metrics"]["restored"] == 1 and restored["metrics"]["new"] == 0
    with duckdb.connect(str(cfg.db_path), read_only=True) as c:
        assert c.execute("SELECT count(*) FROM retirements").fetchone()[0] == 1


def test_stale_archive_cannot_resurrect_or_delete(cfg, tmp_path, log):
    oldcfg = dataclasses.replace(cfg, root=str(tmp_path / "old"))
    old = extract(
        oldcfg, "old", "2025-03-01T00:00:00Z", "full", None, log, Session([page([observation(), observation("b")])])
    )
    ingest(cfg, [observation(), observation("b")])
    ingest(cfg, [observation()], confirm=[])
    before = facts(cfg)
    with pytest.raises(ValueError, match="Stale archive"):
        run_pipeline(cfg, "replay", old)
    assert facts(cfg) == before


def test_large_withdrawal_guard(cfg):
    ingest(cfg, [observation()])
    with pytest.raises(ValueError, match="Withdrawal safety"):
        ingest(dataclasses.replace(cfg, max_withdrawal_fraction=0.25), [], confirm=[])
    assert len(facts(cfg)) == 1


def test_critical_quality_failure_keeps_warehouse_and_checkpoint(cfg):
    ingest(cfg, [observation()])
    before = facts(cfg)
    with duckdb.connect(str(cfg.db_path), read_only=True) as c:
        state = checkpoint(c)
    with pytest.raises(ValueError, match="critical checks"):
        ingest(cfg, [observation("b", gas="CO2")], "incremental")
    assert facts(cfg) == before
    with duckdb.connect(str(cfg.db_path), read_only=True) as c:
        assert checkpoint(c) == state
        assert c.execute("SELECT sum(failures) FROM quality_results WHERE severity='error'").fetchone()[0] == 1


def test_incremental_overlap(cfg):
    import pandas as pd

    ingest(cfg, [observation()])
    with duckdb.connect(str(cfg.db_path), read_only=True) as c:
        state = checkpoint(c)
    s = Session([page([])])
    run_pipeline(cfg, "incremental", session=s)
    lower = s.calls[0]["params"]["modified_at"].split("/")[0]
    assert pd.Timestamp(lower) == pd.Timestamp(state["watermark_utc"]) - pd.Timedelta(hours=48)


def test_interrupted_run_recovered(cfg):
    ingest(cfg, [observation()])
    with duckdb.connect(str(cfg.db_path)) as c:
        c.execute("INSERT INTO pipeline_runs VALUES ('dead','full',now(),NULL,'running',NULL,NULL)")
    ingest(cfg, [], "incremental")
    with duckdb.connect(str(cfg.db_path), read_only=True) as c:
        assert c.execute("SELECT status FROM pipeline_runs WHERE run_id='dead'").fetchone()[0] == "interrupted"


def test_lock_excludes_second_writer_and_releases(cfg):
    with pytest.raises(ValueError):
        with project_lock(cfg.work):
            with pytest.raises(RuntimeError, match="holds"):
                with project_lock(cfg.work):
                    pass
            raise ValueError("interrupted")
    with project_lock(cfg.work):
        pass


def test_incremental_requires_live_baseline(cfg):
    with pytest.raises(ValueError, match="requires"):
        ingest(cfg, [], "incremental")


def test_late_revision_retained_without_replacing_current(cfg):
    for mode, rate, modified in [
        ("full", 100, "2025-02-01"),
        ("incremental", 200, "2025-02-02"),
        ("incremental", 90, "2025-01-31"),
    ]:
        row = observation(emission_auto=rate, modified=modified + "T12:00:00Z")
        run_pipeline(cfg, mode, session=Session([page([row])]))
    with open_warehouse(cfg.db_path, True) as con:
        assert con.execute("SELECT emission_rate_kg_hr FROM fact_observations").fetchone() == (200.0,)
        changes = con.execute(
            "SELECT emission_rate_kg_hr,rate_change_kg_hr FROM mart_revision_history ORDER BY encountered_revision"
        ).fetchall()
        assert changes == [(90.0, None), (100.0, 10.0), (200.0, 100.0)]


def test_global_scope_includes_earliest_emit():
    cfg = load_config(ROOT / "config/global.json")
    assert cfg.start_date == "2022-01-01" and cfg.bbox is None and set(cfg.instruments) == {"emi", "tan"}


def test_new_process_reopens_same_warehouse(tmp_path):
    args = [sys.executable, "-m", "methane", "--config", str(ROOT / "config/demo.json"), "--root", str(tmp_path)]
    for _ in range(2):
        subprocess.run(
            args + ["replay", "--source", str(ROOT / "demo/archive/manifest.json"), "--no-report"],
            capture_output=True,
            check=True,
        )
    result = subprocess.run(
        args + ["query", "--sql", "SELECT count(*) FROM fact_observations"], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip().endswith("42")


def test_delta_requires_contiguous_baseline(cfg, log, tmp_path):
    source = dataclasses.replace(cfg, root=str(tmp_path / "source"))
    delta = extract(
        source, "delta", "2025-04-01T00:00:00Z", "incremental", "2025-03-15T00:00:00Z", log, Session([page([])])
    )
    with pytest.raises(ValueError, match="full baseline"):
        run_pipeline(cfg, "replay", delta)
    full = extract(source, "base", "2025-03-01T00:00:00Z", "full", None, log, Session([page([observation()])]))
    run_pipeline(cfg, "replay", full)
    with pytest.raises(ValueError, match="Archive gap"):
        run_pipeline(cfg, "replay", delta)
    with open_warehouse(cfg.db_path, True) as con:
        assert con.execute("SELECT count(*) FROM fact_observations").fetchone() == (1,)


def test_process_death_before_commit_and_restart(tmp_path):
    config = ROOT / "config/demo.json"
    archive = ROOT / "demo/archive/manifest.json"
    cfg = load_config(config, str(tmp_path))
    code = """import os,sys
import methane.pipeline as pipeline
from methane.config import load_config
pipeline.build_models=lambda *args: os._exit(86)
pipeline.run_pipeline(load_config(sys.argv[1],sys.argv[2]),"replay",sys.argv[3])
"""
    died = subprocess.run([sys.executable, "-c", code, str(config), cfg.root, str(archive)], capture_output=True)
    assert died.returncode == 86
    with open_warehouse(cfg.db_path) as con:
        assert con.execute("SELECT count(*) FROM fact_observations").fetchone() == (0,)
        assert con.execute("SELECT watermark_utc,applied_cutoff FROM checkpoint").fetchone() == (None, None)
    result = run_pipeline(cfg, "replay", archive)
    assert result["metrics"]["current_observations"] == result["metrics"]["history_versions"] == 42
    with open_warehouse(cfg.db_path, True) as con:
        assert con.execute("SELECT status FROM pipeline_runs ORDER BY started_at").fetchall() == [
            ("interrupted",),
            ("succeeded",),
        ]


def test_repeated_confirmed_withdrawal_skips_retired_rows(cfg):
    cfg = dataclasses.replace(cfg, max_withdrawal_fraction=0.25)
    items = [observation(str(i)) for i in range(4)]
    run_pipeline(cfg, "full", session=Session([page(items)]))
    result = run_pipeline(cfg, "full", session=Session([page(items[:3]), page([])]))
    assert result["metrics"]["removed"] == 1
    repeat = run_pipeline(cfg, "replay", result["manifest"])
    assert repeat["metrics"]["removed"] == 0 and repeat["metrics"]["current_observations"] == 3


@pytest.mark.parametrize("failure_at", ["load_current", "build_models", "warehouse_checks"])
def test_transaction_failure_preserves_facts_versions_and_watermark(cfg, monkeypatch, failure_at):
    import methane.pipeline as pipeline

    ingest(cfg, [observation()])
    before = facts(cfg)
    with open_warehouse(cfg.db_path, True) as con:
        state = checkpoint(con)
    original = getattr(pipeline, failure_at)

    def broken(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError("synthetic failure after database writes")

    monkeypatch.setattr(pipeline, failure_at, broken)
    with pytest.raises(OSError):
        ingest(cfg, [observation("b")], "incremental")
    assert facts(cfg) == before
    with open_warehouse(cfg.db_path, True) as con:
        assert checkpoint(con) == state
        assert con.execute("SELECT count(*) FROM observation_versions").fetchone() == (1,)
        assert con.execute("SELECT status FROM pipeline_runs ORDER BY started_at DESC LIMIT 1").fetchone() == (
            "failed",
        )


def test_replay_directory_resumes_and_rejects_gaps(cfg, tmp_path, log):
    source = dataclasses.replace(cfg, root=str(tmp_path / "source"))
    base = extract(source, "base", "2025-03-01T00:00:00Z", "full", None, log, Session([page([observation()])]))
    delta = extract(
        source,
        "delta",
        "2025-03-02T00:00:00Z",
        "incremental",
        "2025-02-27T00:00:00Z",
        log,
        Session([page([observation("b")])]),
    )
    run_pipeline(cfg, "replay", base)
    first = replay_archives(cfg, source.work / "bronze")
    repeated = replay_archives(cfg, source.work / "bronze")
    assert first["metrics"]["current_observations"] == 2
    assert repeated["metrics"]["new"] == repeated["metrics"]["updated"] == 0
    with open_warehouse(cfg.db_path, True) as con:
        assert checkpoint(con)["watermark_utc"] is None
    assert Path(first["manifest"]).read_bytes() == delta.read_bytes()


def test_sql_uses_parquet_and_preserves_quality_and_geospatial_grain(cfg):
    result = ingest(
        cfg,
        [
            observation(),
            observation("b", scene_timestamp="2025-01-02T12:00:00Z"),
            observation("c", emission_auto=None, emission_uncertainty_auto=None, plume_quality=None),
        ],
    )
    import pandas as pd
    from methane.common import sha_file

    parquet = cfg.work / result["metrics"]["parquet"]
    assert sha_file(parquet) == result["metrics"]["parquet_sha256"]
    frame = pd.read_parquet(parquet)
    assert str(frame.observed_at.dtype).startswith("datetime64[ns, UTC]")
    with open_warehouse(cfg.db_path, True) as con:
        assert con.execute("SELECT sum(observations) FROM mart_monthly").fetchone() == (3,)
        assert con.execute(
            "SELECT observations, eligible_observation_days, unrated_observations FROM mart_locations"
        ).fetchone() == (3, 2, 1)
        assert con.execute("SELECT count(*) FROM dim_location").fetchone() == (1,)
        assert con.execute("SELECT count(*) FROM mart_revision_history").fetchone() == (3,)
        assert con.execute("SELECT sum(failures) FROM quality_results WHERE severity='error'").fetchone() == (0,)


def test_auto_uses_incremental_then_periodic_full(cfg):
    ingest(cfg, [observation()])
    assert ingest(cfg, [], "auto")["metrics"]["mode"] == "incremental"
    with open_warehouse(cfg.db_path) as con:
        con.execute("UPDATE checkpoint SET last_full_utc=now()-INTERVAL '8 days'")
    assert ingest(cfg, [observation()], "auto")["metrics"]["mode"] == "full"


def test_complete_historical_snapshot_and_repeat_delta(tmp_path):
    from methane.common import sha_file

    cfg = load_config(ROOT / "config/global.json", str(tmp_path))
    index = json.loads((ROOT / "source_manifests/historical.json").read_text(encoding="utf-8"))
    for entry in index["archives"]:
        assert sha_file(ROOT / entry["manifest"]) == entry["sha256"]
    first = replay_archives(cfg, ROOT / "archives")
    again = replay_archives(cfg, ROOT / "archives")
    assert first["metrics"]["extracted_rows"] == 129  # The archived overlapping delta.
    assert again["metrics"]["new"] == again["metrics"]["updated"] == again["metrics"]["removed"] == 0
    with open_warehouse(cfg.db_path, True) as con:
        assert con.execute("SELECT count(*) FROM fact_observations").fetchone() == (21329,)
        assert con.execute("SELECT count(*) FROM observation_versions").fetchone() == (21329,)
        assert con.execute(
            "SELECT count(DISTINCT scene_id), count(DISTINCT h3_cell) FROM fact_observations"
        ).fetchone() == (8213, 7226)
        assert con.execute("SELECT instrument, count(*) FROM fact_observations GROUP BY 1 ORDER BY 1").fetchall() == [
            ("emi", 4311),
            ("tan", 17018),
        ]
        assert con.execute("SELECT count(*) FROM mart_locations WHERE eligible_observation_days>=2").fetchone() == (
            2814,
        )
        assert con.execute("SELECT count(*) FROM fact_observations WHERE plume_quality='not_supplied'").fetchone() == (
            21329,
        )
        assert con.execute("SELECT count(*) FROM fact_observations WHERE emission_rate_kg_hr IS NULL").fetchone() == (
            4479,
        )
        con.execute((ROOT / "src/methane/sql/analyses.sql").read_text(encoding="utf-8"))
