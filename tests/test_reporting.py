from pathlib import Path
import json
import pandas as pd
import pytest
from methane.config import load_config
from methane.pipeline import run_pipeline
from methane.reporting import generate_report
from methane.warehouse import open_warehouse, checkpoint
from conftest import observation, Session, page

ROOT = Path(__file__).parents[1]
pytestmark = pytest.mark.integration


def test_real_archive_replay_twice_and_offline_report(tmp_path):
    cfg = load_config(ROOT / "config/demo.json", str(tmp_path / "data"))
    source = ROOT / "demo/archive/manifest.json"
    first = run_pipeline(cfg, "replay", source)
    second = run_pipeline(cfg, "replay", source)
    assert first["metrics"]["history_versions"] == second["metrics"]["current_observations"] == 42
    assert second["metrics"]["new"] == second["metrics"]["updated"] == 0
    output = tmp_path / "reports"
    path = Path(generate_report(cfg, output))
    assert path.is_file() and (output / "map.html").is_file()
    assert len(pd.read_parquet(output / "fact_observations.parquet")) == 42
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert summary["current_observations"] == 42 and summary["unrated_observations"] == 42
    document = path.read_text(encoding="utf-8")
    assert "Plotly.newPlot" in document and "srcdoc=" in document and "Carbon Mapper" in document
    assert '<script src="http' not in document and '<link href="http' not in document
    with open_warehouse(cfg.db_path, True) as con:
        assert checkpoint(con)["watermark_utc"] is None


def test_report_failure_leaves_committed_data_and_previous_html(cfg, tmp_path, monkeypatch):
    run_pipeline(cfg, "full", session=Session([page([observation()])]))
    output = tmp_path / "reports"
    report = Path(generate_report(cfg, output))
    previous = report.read_bytes()
    with open_warehouse(cfg.db_path, True) as con:
        state = checkpoint(con)
    original = pd.DataFrame.to_csv
    with monkeypatch.context() as m:

        def failed(*args, **kwargs):
            raise OSError("synthetic export failure")

        m.setattr(pd.DataFrame, "to_csv", failed)
        with pytest.raises(OSError):
            generate_report(cfg, output)
    assert report.read_bytes() == previous
    with open_warehouse(cfg.db_path, True) as con:
        assert checkpoint(con) == state
        assert con.execute("SELECT status FROM pipeline_runs").fetchone() == ("succeeded",)
    assert pd.DataFrame.to_csv is original
    assert Path(generate_report(cfg, output)).is_file()


def test_empty_scope_keeps_typed_tables_and_renders(cfg, tmp_path):
    result = run_pipeline(cfg, "full", session=Session([page([])]))
    assert result["metrics"]["current_observations"] == 0
    assert Path(generate_report(cfg, tmp_path / "reports")).is_file()
