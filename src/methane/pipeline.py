"""Archive, validate and commit one batch. DuckDB owns the only checkpoint."""

import json
import os
import shutil
import tempfile
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from .common import iso, utcnow, write_json, sha_file, project_lock, RunLog
from .api import extract, read_archive
from .normalize import normalize
from .warehouse import open_warehouse, initialize_db, load_current, build_models, warehouse_checks, checkpoint


def stage_replay(source, cfg):
    """Verify an archive, then keep an unchanged local copy beside the warehouse."""
    source = Path(source).resolve()
    manifest, records = read_archive(source, cfg.scope_id)
    del records
    dest = cfg.work / "bronze" / manifest["run_id"]
    if dest.exists():
        if not (dest / "manifest.json").is_file() or sha_file(dest / "manifest.json") != sha_file(source):
            raise ValueError("Archive run ID collision or incomplete local archive; use a fresh root")
    else:
        if any(p.is_symlink() for p in source.parent.rglob("*")):
            raise ValueError("Symlinks are not allowed in imported archives")
        dest.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".import-", dir=dest.parent) as temp:
            staged = Path(temp) / "archive"
            shutil.copytree(source.parent, staged)
            os.replace(staged, dest)
    return dest / "manifest.json"


def run_pipeline(cfg, mode="auto", replay_manifest=None, session=None):
    if mode not in {"auto", "full", "incremental", "replay"}:
        raise ValueError("Use auto, full, incremental, or replay")
    with project_lock(cfg.work), open_warehouse(cfg.db_path) as con:
        initialize_db(con)
        # The OS releases the writer lock on process death; DuckDB rolls back its transaction.
        con.execute("UPDATE pipeline_runs SET status='interrupted', finished_at=now() WHERE status='running'")
        return _run_batch(con, cfg, mode, replay_manifest, session)


def _run_batch(con, cfg, mode, replay_manifest, session):
    started = time.monotonic()
    now = iso(utcnow())
    rid = utcnow().strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid.uuid4().hex[:8]
    log = RunLog(cfg.work / "logs" / f"{rid}.jsonl", rid)
    state = checkpoint(con)
    if mode == "auto":
        due = not state["last_full_utc"] or (utcnow() - datetime.fromisoformat(state["last_full_utc"])) >= timedelta(
            days=cfg.full_refresh_days
        )
        mode = "full" if due else "incremental"
    con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, NULL, 'running', NULL, NULL)", [rid, mode, now])
    transaction = False
    checks, metrics = [], {"run_id": rid, "mode": mode, "scope_id": cfg.scope_id}
    try:
        log.event("run_started", mode=mode, scope_id=cfg.scope_id)
        if mode == "incremental" and not state["watermark_utc"]:
            raise ValueError("Incremental requires a successful live full ingestion first")
        lower = (
            iso(datetime.fromisoformat(state["watermark_utc"]) - timedelta(hours=cfg.overlap_hours))
            if mode == "incremental"
            else None
        )
        if mode == "replay":
            if not replay_manifest:
                raise ValueError("Replay requires a source manifest")
            path = stage_replay(replay_manifest, cfg)
        else:
            ids = [r[0] for r in con.execute("SELECT observation_id FROM fact_observations").fetchall()]
            path = extract(cfg, rid, now, mode, lower, log, session=session, known_ids=ids)
        manifest, records = read_archive(path, cfg.scope_id)
        cutoff = manifest["cutoff_utc"]
        if mode == "replay" and manifest["mode"] == "incremental":
            if not state["applied_cutoff"]:
                raise ValueError("An incremental archive requires a full baseline in this warehouse")
            lower = manifest.get("lower_modified_utc")
            if not lower or datetime.fromisoformat(lower) > datetime.fromisoformat(state["applied_cutoff"]):
                raise ValueError("Archive gap: the modification window starts after the previous cutoff")
        if state["applied_cutoff"] and datetime.fromisoformat(cutoff) < datetime.fromisoformat(state["applied_cutoff"]):
            raise ValueError("Stale archive: replay older cutoffs into a fresh root; current warehouse preserved")
        frame, rejected, checks = normalize(records, cfg)
        quality_dir = cfg.work / "quality" / rid
        write_json(quality_dir / "quarantine.json", rejected)
        metrics.update(
            extracted_rows=len(records),
            invalid_records=len(rejected),
            cutoff_utc=cutoff,
            raw_pages=len(manifest["pages"]),
            manifest=path.relative_to(cfg.work).as_posix(),
            manifest_sha256=sha_file(path),
        )
        if rejected:
            raise ValueError(f"{len(rejected)} records failed critical checks; inspect {quality_dir}/quarantine.json")
        full = manifest["mode"] == "full"
        absent = manifest.get("confirmed_absent", []) if full else []
        old_count = con.execute("SELECT count(*) FROM fact_observations").fetchone()[0]
        pending = con.execute(
            """SELECT count(*) FROM observation_state WHERE active AND last_seen_cutoff < CAST(? AS TIMESTAMPTZ)
            AND observation_id IN (SELECT unnest(?::VARCHAR[]))""",
            [cutoff, absent],
        ).fetchone()[0]
        if old_count and pending / old_count > cfg.max_withdrawal_fraction:
            raise ValueError(
                "Withdrawal safety threshold exceeded; inspect archive before changing max_withdrawal_fraction"
            )
        silver = cfg.work / "silver" / f"{rid}.parquet"
        silver.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(silver, index=False)
        with silver.open("rb") as f:
            os.fsync(f.fileno())
        metrics.update(parquet=silver.relative_to(cfg.work).as_posix(), parquet_sha256=sha_file(silver))
        con.execute("BEGIN TRANSACTION")
        transaction = True
        changes = load_current(con, silver, rid, cutoff, full_refresh=full, confirmed_absent=absent)
        build_models(con)
        checks.extend(warehouse_checks(con))
        con.executemany(
            "INSERT INTO quality_results VALUES (?, ?, ?, ?)",
            [(rid, x["check"], x["severity"], x["failures"]) for x in checks],
        )
        metrics.update(
            changes,
            current_observations=con.execute("SELECT count(*) FROM fact_observations").fetchone()[0],
            history_versions=con.execute("SELECT count(*) FROM observation_versions").fetchone()[0],
            duration_seconds=round(time.monotonic() - started, 3),
            **log.counts,
        )
        # Replay advances archive ordering only. It never establishes a live API watermark.
        con.execute(
            """UPDATE checkpoint SET watermark_utc=CASE WHEN ? THEN watermark_utc ELSE CAST(? AS TIMESTAMPTZ) END,
            last_full_utc=CASE WHEN ? THEN CAST(? AS TIMESTAMPTZ) ELSE last_full_utc END,
            applied_cutoff=CAST(? AS TIMESTAMPTZ), last_successful_run=?""",
            [mode == "replay", now, mode == "full", now, cutoff, rid],
        )
        con.execute(
            "UPDATE pipeline_runs SET status='succeeded', finished_at=now(), metrics_json=? WHERE run_id=?",
            [json.dumps(metrics), rid],
        )
        con.execute("COMMIT")
        transaction = False
    except Exception as exc:
        if transaction:
            con.execute("ROLLBACK")
        checks.extend(getattr(exc, "checks", []))
        error = f"{type(exc).__name__}: {str(exc).split('https://')[0]}"[:1000]
        metrics.update(log.counts, duration_seconds=round(time.monotonic() - started, 3))
        con.execute(
            "UPDATE pipeline_runs SET status='failed', finished_at=now(), metrics_json=?, error=? WHERE run_id=?",
            [json.dumps(metrics), error, rid],
        )
        for x in checks:
            con.execute(
                "INSERT OR REPLACE INTO quality_results VALUES (?, ?, ?, ?)",
                [rid, x["check"], x["severity"], x["failures"]],
            )
        log.event("run_failed", error=error)
        raise
    log.event("run_succeeded", **changes, current_observations=metrics["current_observations"])
    return {"database": str(cfg.db_path), "manifest": str(path), "metrics": metrics}


def replay_archives(cfg, source):
    """Resume an ordered directory of archives, or replay one explicitly selected manifest."""
    source = Path(source)
    if source.is_file():
        return run_pipeline(cfg, "replay", source)
    manifests = []
    for path in source.rglob("manifest.json"):
        m = json.loads(path.read_text(encoding="utf-8"))
        if m.get("scope_id") == cfg.scope_id and m.get("complete"):
            manifests.append((datetime.fromisoformat(m["cutoff_utc"]), path))
    manifests.sort()
    if not manifests:
        raise ValueError("No complete archives for this config; check --source and --config")
    if cfg.db_path.exists():
        with project_lock(cfg.work), open_warehouse(cfg.db_path, read_only=True) as con:
            last = checkpoint(con)["applied_cutoff"]
        if last:
            manifests = [(cutoff, path) for cutoff, path in manifests if cutoff >= datetime.fromisoformat(last)]
    if not manifests:
        raise ValueError("All source archives are older than this warehouse; use --root with a fresh directory")
    for _, path in manifests:
        result = run_pipeline(cfg, "replay", path)
    return result
