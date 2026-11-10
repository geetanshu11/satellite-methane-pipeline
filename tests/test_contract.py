import dataclasses
import pytest
from methane.normalize import normalize, timestamp, stable_value
from methane.common import digest
from methane.config import Config
from conftest import observation


@pytest.mark.parametrize(
    "updates,reason",
    [
        ({"id": None}, "missing_id"),
        ({"id": "bad-id"}, "missing_id"),
        ({"id": 123}, "missing_id"),
        ({"plume_id": ""}, "missing_id"),
        ({"plume_id": "  "}, "missing_id"),
        ({"gas": "CO2"}, "wrong_gas"),
        ({"instrument": "ang"}, "wrong_instrument"),
        ({"status": "hidden"}, "not_published"),
        ({"hide_emission": 1}, "missing_visibility_flag"),
        ({"scene_id": "not-uuid"}, "missing_scene_id"),
        ({"geometry_json": None}, "invalid_geometry_type"),
        ({"geometry_json": {"type": "LineString", "coordinates": [-121, 37]}}, "invalid_geometry_type"),
        ({"geometry_json": {"type": "Point", "coordinates": [37, -121]}}, "invalid_coordinates"),
        ({"geometry_json": {"type": "Point", "coordinates": ["bad", 37]}}, "invalid_coordinates"),
        ({"scene_timestamp": "bad"}, "invalid_required_timestamp"),
        ({"scene_timestamp": "2025-01-01T00:00:00"}, "invalid_required_timestamp"),
        ({"modified": "2024-01-01T00:00:00Z"}, "modified_before_observation"),
        ({"modified": "2099-01-01T00:00:00Z"}, "future_timestamp"),
        ({"published_at": "oops"}, "invalid_published_timestamp"),
        ({"published_at": "2024-12-31T23:00:00Z"}, "invalid_published_timestamp"),
        ({"plume_quality": ["good"]}, "invalid_plume_quality_type"),
        ({"sector": {"code": "6A"}}, "invalid_sector_type"),
    ],
)
def test_critical_records_quarantined(cfg, updates, reason):
    df, rejected, checks = normalize([(observation(**updates), "raw")], cfg)
    assert df.empty and reason in rejected[0]["reasons"] and checks[0]["failures"] == 1


@pytest.mark.parametrize("bad", [None, [], "str", 5])
def test_malformed_record(cfg, bad):
    assert normalize([(bad, "raw")], cfg)[1][0]["reasons"] == ["malformed_record"]


@pytest.mark.parametrize("value", [-1, "nonsense", "NaN", "Infinity", True])
@pytest.mark.parametrize(
    "field,col", [("emission_auto", "emission_rate_kg_hr"), ("emission_uncertainty_auto", "uncertainty_kg_hr")]
)
def test_bad_optional_estimates_remain_missing(cfg, value, field, col):
    df, rejected, _ = normalize([(observation(**{field: value}), "raw")], cfg)
    assert not rejected and df[col].isna().all()
    assert "invalid_" in df.quality_flags.iloc[0]


def test_hidden_estimates(cfg):
    df, _, _ = normalize([(observation(hide_emission=True), "raw")], cfg)
    assert df.emission_rate_kg_hr.isna().all() and df.uncertainty_kg_hr.isna().all()
    assert not df.review_eligible.any()


def test_unknown_quality_visible(cfg):
    df, _, _ = normalize([(observation(plume_quality=None), "raw")], cfg)
    assert df.plume_quality.iloc[0] == "not_supplied" and "quality_not_supplied" in df.quality_flags.iloc[0]


def test_coordinate_order_and_bbox(cfg):
    import h3

    df, _, _ = normalize([(observation(), "raw")], cfg)
    assert df.h3_cell.iloc[0] == h3.latlng_to_cell(37.4, -121.5, 8)
    _, rejected, _ = normalize([(observation(), "raw")], dataclasses.replace(cfg, bbox=(-10, 0, 10, 10)))
    assert "outside_requested_box" in rejected[0]["reasons"]


def test_duplicate_ids_rejected(cfg):
    with pytest.raises(ValueError, match="Duplicate"):
        normalize([(observation(), "raw"), (observation(), "raw")], cfg)


def test_timestamp_offset_and_invalid():
    assert timestamp("2025-01-01T07:00:00-05:00") == timestamp("2025-01-01T12:00:00Z")
    assert timestamp(1735732800) is None


def test_signatures_do_not_create_revisions():
    a = observation(plume_png="https://catalog.carbonmapper.org/p.png?item=1&Expires=1&Signature=old")
    b = observation(plume_png="https://catalog.carbonmapper.org/p.png?item=1&Expires=2&Signature=new")
    assert digest(stable_value(a)) == digest(stable_value(b))
    b["emission_auto"] = 200
    assert digest(stable_value(a)) != digest(stable_value(b))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"page_size": 0},
        {"page_size": 1001},
        {"max_retries": -1},
        {"bbox": (10, 0, -10, 5)},
        {"instruments": ["ang"]},
        {"start_date": "2025-01-01T00:00:00Z"},
        {"end_date": "2021-01-01"},
        {"max_withdrawal_fraction": 1.1},
    ],
)
def test_invalid_configuration(kwargs):
    with pytest.raises(ValueError):
        Config(**kwargs)


def test_scope_separates_filters_not_transport(cfg):
    assert cfg.scope_id == dataclasses.replace(cfg, page_size=10).scope_id
    assert cfg.scope_id != dataclasses.replace(cfg, end_date="2026-01-01").scope_id
