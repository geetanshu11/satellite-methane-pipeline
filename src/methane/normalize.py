import math
from datetime import datetime
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
import pandas as pd
import h3
from .common import digest, SECTORS, utcnow
from .api import valid_uuid

DATA_TYPES = {
    "observation_id": "VARCHAR",
    "plume_id": "VARCHAR",
    "gas": "VARCHAR",
    "scene_id": "VARCHAR",
    "observed_at": "TIMESTAMPTZ",
    "modified_at": "TIMESTAMPTZ",
    "published_at": "TIMESTAMPTZ",
    "instrument": "VARCHAR",
    "platform": "VARCHAR",
    "sector_code": "VARCHAR",
    "longitude": "DOUBLE",
    "latitude": "DOUBLE",
    "emission_rate_kg_hr": "DOUBLE",
    "uncertainty_kg_hr": "DOUBLE",
    "wind_speed_m_s": "DOUBLE",
    "wind_direction_deg": "DOUBLE",
    "plume_quality": "VARCHAR",
    "hide_emission": "BOOLEAN",
    "review_eligible": "BOOLEAN",
    "processing_version": "VARCHAR",
    "h3_cell": "VARCHAR",
    "quality_flags": "VARCHAR",
    "record_hash": "VARCHAR",
    "raw_page": "VARCHAR",
}


def stable_value(value):
    # Expiring image signatures are transport details, not new scientific observations.
    if isinstance(value, dict):
        return {k: stable_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [stable_value(v) for v in value]
    if isinstance(value, str) and value.startswith("https://"):
        parts = urlsplit(value)
        keep = [
            (k, v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k.lower() not in {"expires", "signature", "key-pair-id"} and not k.lower().startswith("x-amz-")
        ]
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(keep), parts.fragment))
    return value


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        val = float(value)
        return val if math.isfinite(val) else None
    except (TypeError, ValueError):
        return None


def timestamp(value):
    # A scalar Timestamp avoids repeating pandas' general date-inference work for every field.
    if not isinstance(value, (str, datetime, pd.Timestamp)):
        return None
    try:
        val = pd.Timestamp(value)
        if pd.isna(val):
            return None
        return None if val.tzinfo is None else val.tz_convert("UTC")
    except (TypeError, ValueError):
        return None


def normalize(records, cfg):
    rows, rejected = [], []
    for item, raw_page in records:
        errors, flags = [], []
        if not isinstance(item, dict):
            rejected.append({"observation_id": None, "reasons": ["malformed_record"], "raw_page": raw_page})
            continue
        oid, name = item.get("id"), item.get("plume_id")
        if not valid_uuid(oid) or not isinstance(name, str) or not name.strip():
            errors.append("missing_id")
        if item.get("gas") != "CH4":
            errors.append("wrong_gas")
        if item.get("instrument") not in cfg.instruments:
            errors.append("wrong_instrument")
        if item.get("status") != "published":
            errors.append("not_published")
        geometry = item.get("geometry_json")
        if not isinstance(geometry, dict):
            geometry = {}
        if geometry.get("type") != "Point":
            errors.append("invalid_geometry_type")
        coords = geometry.get("coordinates", [])
        lon = number(coords[0]) if isinstance(coords, list) and len(coords) >= 2 else None
        lat = number(coords[1]) if isinstance(coords, list) and len(coords) >= 2 else None
        if lon is None or lat is None or not (-180 <= lon <= 180 and -90 <= lat <= 90):
            errors.append("invalid_coordinates")
        elif cfg.bbox:
            w, s, e, n = cfg.bbox
            if not (w <= lon <= e and s <= lat <= n):
                errors.append("outside_requested_box")
        observed, modified = timestamp(item.get("scene_timestamp")), timestamp(item.get("modified"))
        if observed is None or modified is None:
            errors.append("invalid_required_timestamp")
        elif observed < pd.Timestamp(cfg.start_date, tz="UTC"):
            errors.append("before_requested_period")
        elif cfg.end_date and observed > pd.Timestamp(cfg.end_date, tz="UTC"):
            errors.append("after_requested_period")
        if observed is not None and modified is not None:
            if modified < observed:
                errors.append("modified_before_observation")
            if max(observed, modified) > pd.Timestamp(utcnow()) + pd.Timedelta(minutes=5):
                errors.append("future_timestamp")
        published = timestamp(item.get("published_at"))
        if item.get("published_at") is not None:
            if (
                published is None
                or (observed is not None and published < observed)
                or published > pd.Timestamp(utcnow()) + pd.Timedelta(minutes=5)
            ):
                errors.append("invalid_published_timestamp")
        for key in ("plume_quality", "sector"):
            if item.get(key) is not None and not isinstance(item[key], str):
                errors.append("invalid_" + key + "_type")
        if not valid_uuid(item.get("scene_id")):
            errors.append("missing_scene_id")
        if not isinstance(item.get("hide_emission"), bool):
            errors.append("missing_visibility_flag")
        if errors:
            rejected.append({"observation_id": oid, "reasons": errors, "raw_page": raw_page})
            continue
        for key in ("emission_auto", "emission_uncertainty_auto", "wind_speed_avg_auto", "wind_direction_avg_auto"):
            if item.get(key) is not None and number(item[key]) is None:
                flags.append("invalid_" + key)
        hidden = item["hide_emission"]
        rate, uncertainty = number(item.get("emission_auto")), number(item.get("emission_uncertainty_auto"))
        if rate is not None and rate < 0:
            rate = None
            flags.append("invalid_negative_rate")
        if uncertainty is not None and uncertainty < 0:
            uncertainty = None
            flags.append("invalid_negative_uncertainty")
        if hidden:
            rate, uncertainty = None, None
            flags.append("estimate_withheld_by_provider")
        quality = item.get("plume_quality") or "not_supplied"
        if quality not in {"good", "questionable", "bad", "not_supplied"}:
            flags.append("unrecognized_quality_rating")
        if quality == "not_supplied":
            flags.append("quality_not_supplied")
        if rate is None:
            flags.append("rate_unavailable")
        if uncertainty is None:
            flags.append("uncertainty_unavailable")
        sector = item.get("sector") or "unknown"
        if sector not in SECTORS:
            flags.append("unmapped_sector")
        wind, direction = number(item.get("wind_speed_avg_auto")), number(item.get("wind_direction_avg_auto"))
        if wind is not None and wind < 0:
            wind = None
            flags.append("invalid_wind_speed")
        if direction is not None and not 0 <= direction <= 360:
            direction = None
            flags.append("invalid_wind_direction")
        rows.append(
            {
                "observation_id": oid.lower(),
                "plume_id": name,
                "gas": "CH4",
                "scene_id": item["scene_id"].lower(),
                "observed_at": observed,
                "modified_at": modified,
                "published_at": published,
                "instrument": item["instrument"],
                "platform": str(item.get("platform") or "Unknown"),
                "sector_code": sector,
                "longitude": lon,
                "latitude": lat,
                "emission_rate_kg_hr": rate,
                "uncertainty_kg_hr": uncertainty,
                "wind_speed_m_s": wind,
                "wind_direction_deg": direction,
                "plume_quality": quality,
                "hide_emission": hidden,
                "review_eligible": rate is not None and rate > 0 and quality in {"good", "not_supplied"},
                "processing_version": str(item.get("emission_version") or "unknown"),
                "h3_cell": h3.latlng_to_cell(lat, lon, cfg.h3_resolution),
                "quality_flags": ";".join(flags),
                "record_hash": digest(stable_value({**item, "id": oid.lower(), "scene_id": item["scene_id"].lower()})),
                "raw_page": raw_page,
            }
        )
    frame = pd.DataFrame(rows, columns=DATA_TYPES)
    for col, dtype in DATA_TYPES.items():
        if dtype == "TIMESTAMPTZ":
            frame[col] = pd.to_datetime(frame[col], utc=True)
        elif dtype == "DOUBLE":
            frame[col] = pd.to_numeric(frame[col], errors="coerce").astype("float64")
        elif dtype == "BOOLEAN":
            frame[col] = frame[col].astype("bool")
        else:
            frame[col] = frame[col].astype("string")
    if frame.observation_id.duplicated().any():
        raise ValueError("Duplicate observation IDs after normalization.")
    checks = [
        {"check": "critical_contract", "severity": "error", "failures": len(rejected)},
        {
            "check": "quality_rating_not_supplied",
            "severity": "warning",
            "failures": int((frame.plume_quality == "not_supplied").sum()),
        },
        {"check": "rate_unavailable", "severity": "warning", "failures": int(frame.emission_rate_kg_hr.isna().sum())},
        {
            "check": "uncertainty_unavailable",
            "severity": "warning",
            "failures": int(frame.uncertainty_kg_hr.isna().sum()),
        },
    ]
    checks.append(
        {
            "check": "invalid_optional_values",
            "severity": "warning",
            "failures": int(frame.quality_flags.str.contains("invalid_").sum()),
        }
    )
    return frame, rejected, checks
