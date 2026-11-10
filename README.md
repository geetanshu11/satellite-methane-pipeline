# Satellite Methane Monitoring

A Python pipeline for exploring published Carbon Mapper methane observations. It archives API responses, cleans them into Parquet, tracks revisions in DuckDB, and uses SQL and H3 to study observation patterns. Methane detection happens upstream; this project processes the published results.

The included historical snapshot has **21,329 EMIT/Tanager observations**, covering **August 10, 2022–October 6, 2026**. It is a selected satellite scope, not the entire Carbon Mapper catalogue. [Findings](docs/FINDINGS.md) · [Source attribution and terms](DATA_NOTICE.md)

![Historical observation report](docs/images/report.png)

## Run on Windows

Start Docker Desktop with Linux containers. Extract this ZIP, open the folder containing `compose.yaml`, and open PowerShell there. Run:

```powershell
docker compose run --build --rm pipeline
Start-Process .\reports\report.html
```

This builds one application image, replays the **complete archived dataset**, and creates the report plus CSV/Parquet exports in `reports\`. Run it again to check that the observation and version counts stay at 21,329. No Python installation, credentials, AWS account, or paid service is needed.

The setup fits a Windows 11 laptop with 16 GB RAM and a 6 GB WSL limit. The container is capped at 3 GB and two CPUs; DuckDB uses a 1 GB session limit. Database files persist in the `methane-data` Docker volume. Building images requires internet. Historical replay, tests and the self-contained maps/charts work offline afterward.

## Collect live data

Use a separate data directory so the included historical result remains reproducible:

```powershell
docker compose run --rm pipeline --root /data/live --output /reports/live run
Start-Process .\reports\live\report.html
```

The first run collects the full selected scope. Subsequent runs request changes by source modification time. After seven days, the same command performs a full reconciliation. These are manual batch runs; Docker Desktop must be running. Live retrieval requires internet and uses the [documented public API](https://api.carbonmapper.org/api/v1/docs).

For a smaller live scope, add `--config config/regional.json` before `run`. To change dates or geography, copy `config/global.json` to `config/local.json`, edit it, and pass `--config config/local.json`. `bbox` is `[west, south, east, north]`; date endpoints are inclusive UTC midnights. Different filters get separate databases. The original notebook remains under `notebooks/legacy/`.

## How it works

```mermaid
flowchart TD
    A[Carbon Mapper API] --> B[Raw JSON and checksums]
    B --> C[Validate and clean]
    C -->|Invalid record| Q[Quarantine and stop]
    C -->|Valid batch| P[Typed Parquet]
    P --> D[DuckDB transaction]
    D --> H[Current rows and revision history]
    D --> S[SQL marts and quality checks]
    H --> W[Commit with watermark]
    S --> W
    W --> R[Charts, map and exports]
```

Eight core modules keep the steps separate:

| File | Responsibility |
|---|---|
| `api.py` | Pagination, bounded retries, raw response archives and replay verification |
| `normalize.py` | Types, validation, missing-value flags, content hashes and H3 cells |
| `warehouse.py` | Parquet loading, revision updates and SQL transformations |
| `pipeline.py` | Batch ordering, transactions, quality gates and replay |
| `reporting.py` | CSV/Parquet exports, Plotly charts and the offline Folium map |
| `config.py`, `common.py`, `cli.py` | Dataset settings, file/lock helpers and the four commands |

The **watermark** is the start time of the last successful live collection. Each incremental request overlaps it by 48 hours. Pagination checks filtered counts, offsets and duplicate UUIDs; incomplete responses fail the batch. A failed download restarts from page zero on the next attempt.

Each UUID has one current observation. A content hash identifies distinct encountered revisions, ignoring expiring image signatures. Source modification time orders revisions; later capture time breaks ties. Older revisions remain in history without overwriting newer facts. Repeated runs create audit entries but do not duplicate facts or version hashes.

During a full run, missing known IDs are queried again without a modification cutoff. Only confirmed absence retires a current row; history stays. More than 25% pending withdrawals blocks the batch for inspection. Withdrawal means absence from the selected published scope, not that emissions stopped. Incremental requests alone cannot detect withdrawals.

Facts, versions, SQL marts, quality results and the checkpoint commit together. **DuckDB is the only checkpoint**. Raw-page references and manifest/Parquet hashes preserve provenance. Offline replay verifies archived checksums and requires a full baseline before contiguous deltas; it never advances the live watermark. Reports are derived after commit and can be regenerated. There is no exactly-once claim across HTTP, files and DuckDB.

## Commands and tables

```powershell
# Query the historical warehouse
docker compose run --rm pipeline query --sql "SELECT sector_name, observations FROM mart_sectors ORDER BY observations DESC"

# Inspect runs and quality checks
docker compose run --rm pipeline query --sql "SELECT mode, status, started_at, error FROM pipeline_runs ORDER BY started_at DESC LIMIT 5"
docker compose run --rm pipeline query --sql "SELECT * FROM quality_results ORDER BY run_id DESC LIMIT 11"

# Regenerate the historical report, or force live reconciliation
docker compose run --rm pipeline --output /reports report
docker compose run --rm pipeline --root /data/live --output /reports/live run --mode full
```

| Tables | Grain |
|---|---|
| `fact_observations` | One current provider UUID; timestamps, rate/uncertainty in kg/hr, quality flags, H3 cell and raw-page reference |
| `observation_versions`, `mart_revision_history` | UUID + content hash; the view compares encountered rates with `LAG` |
| `observation_state`, `retirements` | Last state per UUID; confirmed absence per run/UUID |
| `dim_platform`, `dim_sector`, `dim_date`, `dim_location` | Instrument/platform pair, sector code, UTC date, H3 cell |
| `mart_monthly`, `mart_sectors`, `mart_locations`, `mart_review_queue` | Month/instrument, sector, occupied H3 cell, eligible investigation cell |
| `pipeline_runs`, `quality_results`, `checkpoint` | Run, run/check, one checkpoint per dataset scope |

[SQL models](src/methane/sql/models.sql) · [Example analysis queries](src/methane/sql/analyses.sql)

## Tests and recovery

Build the test image, then run the suite with networking disabled:

```powershell
docker build --target test -t satellite-methane-test .
docker run --rm --network none --memory 3g satellite-methane-test
```

For Python development, install Python 3.12 and run these from the project folder:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python -m pip install -c requirements-lock.txt ".[dev]"
.\.venv\Scripts\python -m pytest -q
.\.venv\Scripts\python -m ruff check src tests
.\.venv\Scripts\python -m ruff format --check src tests
.\.venv\Scripts\python -m methane replay
```

Verified here: **108 offline tests passed**, including the complete 21,329-row snapshot, revisions, withdrawals, failed transactions and a writer killed before commit. Ruff lint/format and dependency checks passed. Separate old/new historical replays matched all 24 fact columns and every version hash. Offline Chromium rendered both charts, 500 map markers and a popup without external requests. A live regional full/incremental check also passed. Docker Compose configuration validated; **container execution, native Windows execution and hosted GitHub Actions were not available here**.

Press **Ctrl+C** to interrupt a batch and rerun the same command to resume from committed state. The process lock releases when it exits. `docker compose down` keeps the data volume. Keep the database and its journal together.

| Problem | Next step |
|---|---|
| Docker cannot connect | Start Docker Desktop and check `docker info`. |
| Timeout, 429, 5xx or changing page count | Wait and rerun. Prior facts and watermark remain committed. |
| Invalid records or large withdrawal count | Inspect the failed run and its `quality/…/quarantine.json` inside the data volume before changing filters or thresholds. |
| Report/export fails | Fix disk space or permissions, then rerun `report`; ingestion has already committed. |
| Warehouse is lost or damaged | Replay into a fresh directory: `docker compose run --rm pipeline --root /data/rebuilt --output /reports/rebuilt replay --source /archives`. Keep any additional live archives if you need to reconstruct later updates. |

For the optional 42-observation California fixture:

```powershell
docker compose run --rm pipeline --config config/demo.json --output /reports/demo replay --source demo/archive
```

## Publish to GitHub

The repository includes frozen historical inputs in `archives/` (about 36 MB), source manifests, SQL, tests, configuration, and a GitHub Actions workflow. New downloads, databases, exports, local settings and credentials are ignored. Create an empty repository on GitHub, then run:

```powershell
git init -b main
git add .
git status --short
git commit -m "Add satellite methane data pipeline"
$remote = Read-Host "Paste your own empty repository's HTTPS URL"
git remote add origin $remote
git push -u origin main
```

If Git asks for your identity, set `git config user.name "Your name"` and `git config user.email "Your email"`, then retry. Sign in when Git prompts you. No repository has been published as part of this delivery.
