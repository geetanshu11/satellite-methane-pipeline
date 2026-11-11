"""Focused unit tests for investigation selection (no API needed)."""

import pandas as pd
import pytest

from methane.investigations import SOURCE_COLUMNS, build_investigations, distance_km, render_investigation_html


def records(rows):
    return pd.DataFrame([{**{column: None for column in SOURCE_COLUMNS}, **row} for row in rows]).assign(
        observed_at=lambda d: pd.to_datetime(d.observed_at, utc=True)
    )


def loc(cell, lat=1.0, lon=1.0, days=3, peak=400):
    return {
        "h3_cell": cell,
        "center_latitude": lat,
        "center_longitude": lon,
        "eligible_observation_days": days,
        "peak_observed_rate_kg_hr": peak,
    }


def obs(cell, day, key, rate=100, valid=True, instrument="tan"):
    return {
        "h3_cell": cell,
        "observed_at": day,
        "observation_id": key,
        "plume_id": key,
        "scene_id": "scene-" + key,
        "instrument": instrument,
        "sector_code": "1B2",
        "review_eligible": valid,
        "emission_rate_kg_hr": rate,
        "plume_quality": "not_supplied",
    }


def sectors():
    return pd.DataFrame([{"sector_code": "1B2", "sector_name": "Oil and natural gas"}])


def test_exact_dates_rates_and_evidence_rows():
    data = records(
        [
            obs("a", "2024-01-01", "one", 100),
            obs("a", "2024-01-01", "two", 150),
            obs("a", "2024-02-01", "three", 300, instrument="emi"),
            obs("a", "2024-03-01", "four", None, valid=False),
        ]
    )
    cases = build_investigations(data, pd.DataFrame([loc("a")]), sectors())
    assert len(cases) == 1
    case = cases[0]
    assert case["eligible_dates"] == 2 and case["observation_dates"] == 3
    assert case["observation_count"] == 4 and case["scene_count"] == 4
    assert case["median_eligible_rate_kg_hr"] == 150.0
    assert case["missing_rate_observations"] == 1
    assert case["first_eligible_date"] == "2024-01-01"
    assert case["last_eligible_date"] == "2024-02-01"
    assert case["daily"][0]["median_rate_kg_hr"] == 125.0
    assert case["daily"][-1]["median_rate_kg_hr"] is None
    assert set(case["records"].observation_id) == {"one", "two", "three", "four"}
    assert case["sectors"] == ["Oil and natural gas"]


def test_reject_one_date_and_avoid_neighboring_cells():
    data = records(
        [
            obs("a", "2024-01-01", "a1"),
            obs("a", "2024-02-01", "a2"),
            obs("b", "2024-01-01", "b1"),
            obs("b", "2024-02-01", "b2"),
            obs("c", "2024-01-01", "c1"),
            obs("c", "2024-02-01", "c2"),
            obs("d", "2024-01-01", "d1"),
        ]
    )
    cells = pd.DataFrame(
        [
            loc("a", 1, 1),
            loc("b", 1.001, 1.001),
            loc("c", 10, 10, days=2),
            loc("d", 20, 20, days=2),
        ]
    )
    cases = build_investigations(data, cells, sectors(), limit=5, separation_km=10)
    assert [case["h3_cell"] for case in cases] == ["a", "c"]


def test_no_data_or_invalid_arguments():
    assert build_investigations(pd.DataFrame(), pd.DataFrame(), sectors()) == []
    with pytest.raises(ValueError):
        build_investigations(pd.DataFrame(), pd.DataFrame(), sectors(), limit=-1)
    assert 110 < distance_km(0, 0, 1, 0) < 112


def test_deterministic_tie_breaking_and_missing_rate():
    data = records(
        [
            obs("x", "2024-01-01", "x1"),
            obs("x", "2024-02-01", "x2"),
            obs("y", "2024-01-01", "y1", 200),
            obs("y", "2024-02-01", "y2", 200),
        ]
    )
    cells = pd.DataFrame([loc("x", 10, 10, days=2, peak=100), loc("y", 20, 20, days=2, peak=200)])
    assert [case["h3_cell"] for case in build_investigations(data, cells, sectors())] == ["y", "x"]


def test_html_evidence_and_escaping():
    data = records(
        [
            obs("a", "2024-01-01", "a1", 100),
            obs("a", "2024-02-01", "a2", 200),
        ]
    )
    malicious = pd.DataFrame([{"sector_code": "1B2", "sector_name": "<img src=x onerror=alert(1)>"}])
    cases = build_investigations(data, pd.DataFrame([loc("a")]), malicious)
    html = render_investigation_html(cases)
    assert "Case 1" in html and "2024-02-01" in html
    assert "investigations/case_01_a.csv" in html
    assert "&lt;img src=x onerror=alert(1)&gt;" in html
    assert "<img src=x" not in html
    assert "No investigation candidates" in render_investigation_html([])
