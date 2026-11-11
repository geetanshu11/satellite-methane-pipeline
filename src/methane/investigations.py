"""Evidence-focused investigation dossiers for repeatedly observed H3 grid cells.

These are leads for human review, NOT facility detections or continuous
emission estimates. Selection uses independent eligible observation dates.
"""

from __future__ import annotations

from math import asin, cos, radians, sin, sqrt

import pandas as pd

SOURCE_COLUMNS = (
    "observation_id",
    "plume_id",
    "scene_id",
    "observed_at",
    "modified_at",
    "instrument",
    "platform",
    "sector_code",
    "latitude",
    "longitude",
    "emission_rate_kg_hr",
    "uncertainty_kg_hr",
    "plume_quality",
    "review_eligible",
    "quality_flags",
    "record_hash",
    "raw_page",
)


def distance_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    """Approximate great-circle distance between grid-cell centers."""
    a, b = radians(lat_a), radians(lat_b)
    dy, dx = b - a, radians(lon_b - lon_a)
    h = sin(dy / 2) ** 2 + cos(a) * cos(b) * sin(dx / 2) ** 2
    return 12742 * asin(min(1, sqrt(max(0, h))))


def build_investigations(
    observations: pd.DataFrame,
    locations: pd.DataFrame,
    sectors: pd.DataFrame,
    *,
    limit: int = 5,
    separation_km: float = 10,
) -> list[dict]:
    """Select transparent review candidates and their exact supporting observations.

    Ranking: distinct eligible observation dates, then eligible peak rate,
    then stable H3 cell ID. A spatial separation heuristic avoids presenting
    neighboring H3 cells as separate top-level investigations. This is NOT a
    statistical source attribution, a leak ranking, or an emissions inventory.
    """
    if limit < 0 or separation_km < 0:
        raise ValueError("limit and separation_km cannot be negative")
    if limit == 0 or observations.empty or locations.empty:
        return []
    required = set(SOURCE_COLUMNS) | {"h3_cell"}
    if not required.issubset(observations.columns):
        raise ValueError(f"Missing observation columns: {sorted(required - set(observations.columns))}")
    needed = {
        "h3_cell",
        "eligible_observation_days",
        "peak_observed_rate_kg_hr",
        "center_latitude",
        "center_longitude",
    }
    if not needed.issubset(locations.columns):
        raise ValueError(f"Missing location columns: {sorted(needed - set(locations.columns))}")

    ordered = locations.loc[locations["eligible_observation_days"] >= 2].sort_values(
        ["eligible_observation_days", "peak_observed_rate_kg_hr", "h3_cell"],
        ascending=[False, False, True],
        na_position="last",
    )
    sector_lookup = dict(zip(sectors["sector_code"], sectors["sector_name"]))
    chosen = []
    for loc in ordered.itertuples(index=False):
        if len(chosen) >= limit:
            break
        lat, lon = float(loc.center_latitude), float(loc.center_longitude)
        if any(distance_km(lat, lon, prior["latitude"], prior["longitude"]) < separation_km for prior in chosen):
            continue
        cell = str(loc.h3_cell)
        records = observations.loc[observations["h3_cell"] == cell, list(SOURCE_COLUMNS)].copy()
        if records.empty:
            continue
        records["observed_at"] = pd.to_datetime(records["observed_at"], utc=True)
        records = records.sort_values(["observed_at", "observation_id"], kind="stable")
        eligible = records.loc[
            records["review_eligible"].fillna(False)
            & records["emission_rate_kg_hr"].notna()
            & (records["emission_rate_kg_hr"] > 0)
        ]
        eligible_dates = sorted(eligible["observed_at"].dt.strftime("%Y-%m-%d").unique())
        # Use current records as the source of truth if a mart is stale.
        if len(eligible_dates) < 2:
            continue
        dates = records.assign(date_utc=records["observed_at"].dt.strftime("%Y-%m-%d"))
        daily = []
        for day, rows in dates.groupby("date_utc", sort=True):
            rates = pd.to_numeric(
                rows.loc[rows["review_eligible"].fillna(False), "emission_rate_kg_hr"], errors="coerce"
            ).dropna()
            daily.append(
                {
                    "date_utc": day,
                    "observations": int(len(rows)),
                    "eligible_observations": int(len(rates)),
                    "median_rate_kg_hr": round(float(rates.median()), 1) if len(rates) else None,
                    "max_rate_kg_hr": round(float(rates.max()), 1) if len(rates) else None,
                    "instruments": ", ".join(sorted(set(rows["instrument"].dropna().astype(str)))),
                }
            )
        rates = pd.to_numeric(eligible["emission_rate_kg_hr"], errors="coerce").dropna()
        codes = sorted(records["sector_code"].dropna().astype(str).unique())
        case = {
            "rank": len(chosen) + 1,
            "h3_cell": cell,
            "latitude": round(lat, 5),
            "longitude": round(lon, 5),
            "observation_count": int(len(records)),
            "scene_count": int(records["scene_id"].nunique()),
            "observation_dates": len(daily),
            "eligible_dates": len(eligible_dates),
            "eligible_observations": int(len(eligible)),
            "first_date": daily[0]["date_utc"],
            "last_date": daily[-1]["date_utc"],
            "first_eligible_date": eligible_dates[0],
            "last_eligible_date": eligible_dates[-1],
            "instruments": sorted(records["instrument"].dropna().astype(str).unique()),
            "sectors": [sector_lookup.get(code, f"Unmapped ({code})") for code in codes],
            "unrated_observations": int((records["plume_quality"] == "not_supplied").sum()),
            "missing_rate_observations": int(records["emission_rate_kg_hr"].isna().sum()),
            "median_eligible_rate_kg_hr": round(float(rates.median()), 1),
            "min_eligible_rate_kg_hr": round(float(rates.min()), 1),
            "max_eligible_rate_kg_hr": round(float(rates.max()), 1),
            "daily": daily,
            "records": records,
        }
        chosen.append(case)
    return chosen


def render_investigation_html(cases: list[dict]) -> str:
    """Turn the ranked records into accessible, source-linked HTML case studies."""
    from html import escape

    if not cases:
        return (
            "<p>No grid cells have eligible published observations on two "
            "distinct UTC dates in this selected dataset. "
            "No investigation candidates are claimed.</p>"
        )

    cards = []
    for case in cases:
        daily_rows = []
        for row in reversed(case["daily"][-12:]):
            rate = row["median_rate_kg_hr"]
            rate_text = "Not available" if rate is None else f"{rate:,.1f}"
            daily_rows.append(
                f"<tr><td>{escape(row['date_utc'])}</td>"
                f"<td>{row['observations']}</td>"
                f"<td>{row['eligible_observations']}</td>"
                f"<td>{rate_text}</td>"
                f"<td>{escape(row['instruments'])}</td></tr>"
            )
        rows_html = "".join(daily_rows)
        sectors = escape(", ".join(case["sectors"]))
        instruments = escape(", ".join(case["instruments"]))
        source = f"investigations/case_{case['rank']:02d}_{case['h3_cell']}.csv"
        cards.append(
            f"<article class='case' id='case-{case['rank']}'>"
            f"<h3>Case {case['rank']}: {case['latitude']:.3f}°, {case['longitude']:.3f}° "
            f"<small>(grid cell {escape(case['h3_cell'])})</small></h3>"
            f"<p class='lead'><b>Why this area:</b> {case['eligible_dates']} distinct dates "
            f"with a positive, review-eligible published rate across "
            f"{case['eligible_observations']} observations. The cell has "
            f"{case['observation_count']} total observations on "
            f"{case['observation_dates']} dates and {case['scene_count']} distinct scenes.</p>"
            f"<p><b>When:</b> {escape(case['first_date'])} to "
            f"{escape(case['last_date'])}. <b>Instruments:</b> {instruments}. "
            f"<b>Provider-attributed sectors:</b> {sectors}.</p>"
            f"<p><b>Observed eligible rates:</b> median "
            f"{case['median_eligible_rate_kg_hr']:,.1f} kg/hr; range "
            f"{case['min_eligible_rate_kg_hr']:,.1f}–"
            f"{case['max_eligible_rate_kg_hr']:,.1f} kg/hr. "
            f"{case['unrated_observations']} unrated observations; "
            f"{case['missing_rate_observations']} without a published rate. "
            "Individual rates are not averaged into continuous emissions.</p>"
            "<details><summary>See dated evidence (12 most recent dates)</summary>"
            "<div class='table-scroll'><table><thead><tr><th>UTC date</th>"
            "<th>Observations</th><th>Rate-eligible</th><th>Median rate (kg/hr)</th>"
            "<th>Instruments</th></tr></thead><tbody>" + rows_html + "</tbody></table></div></details>"
            f"<p><a href='{escape(source, quote=True)}'>Download all "
            f"{case['observation_count']} underlying records (CSV)</a>. "
            "Check dated imagery, observation opportunities and independently "
            "verified facility inventories before making site-level claims.</p>"
            "<p class='muted'>This cell may contain multiple facilities; "
            "multiple observations on one date count as one date. "
            "Repetition does not establish continuous leakage, a source identity, "
            "or annual emissions.</p></article>"
        )
    return "".join(cards)
