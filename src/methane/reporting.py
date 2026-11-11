import html
import json
import shutil
from pathlib import Path
from .warehouse import open_warehouse
from .common import project_lock
import pandas as pd
from .common import write_json, CONTRACT_VERSION
from .investigations import build_investigations, render_investigation_html


def generate_report(cfg, output="reports"):
    """Derived outputs can always be regenerated after a successful batch."""
    destination = Path(output).resolve()
    if destination.is_relative_to(cfg.work.resolve()):
        raise ValueError("Keep reports outside the active database directory")
    with project_lock(cfg.work), open_warehouse(cfg.db_path, read_only=True) as con:
        row = con.execute(
            "SELECT metrics_json FROM pipeline_runs WHERE status='succeeded' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            raise ValueError("No committed batch available for a report")
        export_products(con, cfg, destination, json.loads(row[0]))
    return str(destination / "report.html")


def export_products(con, cfg, directory, metrics):
    import folium
    import plotly.express as px
    from folium.plugins import MarkerCluster

    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    run_id = metrics["run_id"]
    tables = [
        "fact_observations",
        "dim_platform",
        "dim_sector",
        "dim_date",
        "dim_location",
        "mart_monthly",
        "mart_sectors",
        "mart_locations",
        "mart_review_queue",
    ]
    frames = {}
    for name in tables:
        df = con.execute(f"SELECT * FROM {name}").df()
        df.to_parquet(directory / f"{name}.parquet", index=False)
        df.to_csv(directory / f"{name}.csv", index=False)
        frames[name] = df
    observations = frames["fact_observations"]
    monthly = frames["mart_monthly"].sort_values("observation_month")
    sectors = frames["mart_sectors"].sort_values("observations", ascending=False)
    queue = frames["mart_review_queue"].sort_values("review_order")
    locations = frames["mart_locations"].merge(frames["dim_location"], on="h3_cell", validate="one_to_one")
    cases = build_investigations(observations, locations, frames["dim_sector"])
    # Full source rows and day-level evidence remain inspectable outside the HTML.
    case_dir = directory / "investigations"
    case_dir.mkdir(exist_ok=True)
    case_summaries = []
    for case in cases:
        filename = f"case_{case['rank']:02d}_{case['h3_cell']}.csv"
        case["records"].to_csv(case_dir / filename, index=False)
        external = {key: value for key, value in case.items() if key != "records"}
        external["records_file"] = f"investigations/{filename}"
        case_summaries.append(external)
    write_json(directory / "investigations.json", case_summaries)
    if case_summaries:
        pd.DataFrame(
            [{k: v for k, v in case.items() if k not in {"daily", "instruments", "sectors"}} for case in case_summaries]
        ).to_csv(directory / "investigation_summary.csv", index=False)
    else:
        pd.DataFrame(columns=["rank", "h3_cell", "eligible_dates"]).to_csv(
            directory / "investigation_summary.csv", index=False
        )
    plotted = locations.sort_values(
        ["eligible_observation_days", "peak_observed_rate_kg_hr", "h3_cell"],
        ascending=[False, False, True],
        na_position="last",
    ).head(cfg.max_map_cells)
    assets = Path(__file__).parent / "assets"
    m = folium.Map(location=[20, 5], zoom_start=2, tiles=None, prefer_canvas=True, max_bounds=True)
    # Without a tile layer, the cluster plugin needs an explicit maximum zoom.
    m.options.update(minZoom=1, maxZoom=18)
    m.default_js, m.default_css = [], []
    folium.GeoJson(
        json.loads((assets / "world.geojson").read_text(encoding="utf-8")),
        name="World outline (Natural Earth)",
        style_function=lambda _: {"color": "#91a7a7", "weight": 0.6, "fillColor": "#f6f4ec", "fillOpacity": 1},
    ).add_to(m)
    if cfg.bbox:
        west, south, east, north = cfg.bbox
        m.fit_bounds([[south, west], [north, east]])
    cluster = MarkerCluster(name="Observed grid cells").add_to(m)
    cluster.default_js, cluster.default_css = [], []
    for row in plotted.to_dict("records"):
        rate = row["peak_observed_rate_kg_hr"]
        rate_text = "Unavailable" if pd.isna(rate) else f"{rate:,.0f} kg/hr"
        repeated = row["eligible_observation_days"] >= 2
        color = "#d97706" if repeated else "#087f8c"
        if pd.isna(rate):
            color = "#64748b"
        popup = (
            f"<b>Grid cell {html.escape(row['h3_cell'])}</b><br>"
            f"Observations: {row['observations']}<br>Detection dates: {row['observation_days']}<br>"
            f"Largest eligible observation: {rate_text}<br>"
            f"Unrated observations: {row['unrated_observations']}<br>"
            "A grid cell may contain multiple facilities. Source: Carbon Mapper."
        )
        folium.CircleMarker(
            [row["center_latitude"], row["center_longitude"]],
            radius=5,
            color=color,
            fill=True,
            fill_opacity=0.75,
            popup=folium.Popup(popup, max_width=320),
        ).add_to(cluster)
    # Make the reviewed evidence locations obvious, not lost among hundreds of map points.
    for case in cases:
        folium.CircleMarker(
            [case["latitude"], case["longitude"]],
            radius=11,
            color="#bd1e40",
            weight=3,
            fill=False,
            tooltip=f"Investigation {case['rank']}: {case['eligible_dates']} eligible dates",
            popup=f"Case {case['rank']}: H3 cell {html.escape(case['h3_cell'])}",
        ).add_to(m)
    folium.LayerControl().add_to(m)
    css = (assets / "map.css").read_text(encoding="utf-8")
    js = (assets / "map.js").read_text(encoding="utf-8")
    embedded = f"<style>{css} .leaflet-container{{background:#dcecf1}}</style><script>{js}</script>"
    map_document = m.get_root().render().replace("</head>", embedded + "</head>", 1)
    (directory / "map.html").write_text(map_document, encoding="utf-8")
    shutil.copy2(assets / "NOTICE.txt", directory / "MAP_NOTICE.txt")

    fig = px.bar(
        monthly,
        x="observation_month",
        y="observations",
        color="instrument",
        labels={"observation_month": "Observation month", "observations": "Published plume observations"},
        title="Catalog observations over time",
    )
    fig.update_layout(template="plotly_white", font_family="Arial", margin=dict(l=30, r=20, t=60, b=30))
    chart1 = fig.to_html(full_html=False, include_plotlyjs=True)
    fig2 = px.bar(
        sectors,
        x="sector_name",
        y="observations",
        title="Observations by provider-attributed sector",
        labels={"sector_name": "Sector", "observations": "Observations"},
    )
    fig2.update_layout(template="plotly_white", font_family="Arial", margin=dict(l=30, r=20, t=60, b=30))
    chart2 = fig2.to_html(full_html=False, include_plotlyjs=False)
    repeated_count = int((locations.eligible_observation_days >= 2).sum())
    summary = {
        **metrics,
        "current_observations": len(observations),
        "grid_cells": len(locations),
        "repeated_detection_cells": repeated_count,
        "mapped_cells": len(plotted),
        "earliest_observation": str(observations.observed_at.min()),
        "latest_observation": str(observations.observed_at.max()),
        "unrated_observations": int((observations.plume_quality == "not_supplied").sum()),
        "missing_rate_observations": int(observations.emission_rate_kg_hr.isna().sum()),
        "missing_uncertainty_observations": int(observations.uncertainty_kg_hr.isna().sum()),
        "investigation_cases": len(cases),
        "source": "Carbon Mapper",
        "scope": cfg.scope,
    }
    write_json(directory / "summary.json", summary)
    view = (
        queue[
            [
                "review_order",
                "h3_cell",
                "eligible_observation_days",
                "peak_observed_rate_kg_hr",
                "unrated_observations",
                "sector_codes",
            ]
        ]
        .head(25)
        .copy()
    )
    view.columns = [
        "Order",
        "Grid cell",
        "Eligible detection dates",
        "Peak observed kg/hr",
        "Unrated observations",
        "Sectors",
    ]
    table_html = view.to_html(index=False, escape=True, float_format=lambda x: f"{x:,.1f}")
    case_html = render_investigation_html(cases)
    map_src = html.escape(map_document, quote=True)
    document = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1"><title>Satellite Methane Investigation Report</title>
    <style>body{{margin:0;background:#f4f7fa;color:#172b3a;font:16px/1.55 Arial,sans-serif}}
    main{{max-width:1180px;margin:auto;padding:32px}}header{{background:#103d48;color:white;padding:32px;border-radius:16px}}
    h1{{font-size:34px;margin:4px 0}}h2{{font-size:23px}}.eyebrow{{letter-spacing:.15em;font-size:12px}}
    .cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin:24px 0}}
    .card,section{{background:white;padding:22px;border-radius:12px;margin-bottom:20px}}.number{{font-size:30px;font-weight:bold}}
    .muted{{color:#526574;font-size:14px}}.note{{border-left:4px solid #d97706;padding:14px;background:#fff7e8}}
    iframe{{width:100%;height:530px;border:0}}.table-scroll{{overflow-x:auto}}table{{border-collapse:collapse;width:100%;font-size:13px}}
    th,td{{padding:10px;text-align:left;border-bottom:1px solid #e2e8f0}}th{{background:#eaf3f5}}a{{color:#087f8c}}
    .case{{border:1px solid #d2e0e5;border-radius:12px;padding:18px;margin:16px 0}}
    .case h3{{font-size:18px;margin:0 0 10px}}.case small{{color:#526574;font-size:12px}}
    .case p{{margin:10px 0}}.case .lead{{background:#f0f7f9;padding:12px;border-radius:8px}}
    details{{margin:12px 0}}summary{{font-weight:bold;cursor:pointer}}
    </style></head><body><main><header><div class="eyebrow">ENVIRONMENTAL DATA ENGINEERING</div>
    <h1>Satellite Methane Investigation Leads</h1><p>Published methane observations, revision history, and a spatial review queue.</p>
    <div>Source: Carbon Mapper · Input archive cutoff {html.escape(metrics["cutoff_utc"])}</div></header>
    <div class="cards"><div class="card">Observations<div class="number">{len(observations):,}</div></div>
    <div class="card">Occupied grid cells<div class="number">{len(locations):,}</div></div>
    <div class="card">Repeated detection cells<div class="number">{repeated_count:,}</div></div>
    <div class="card">Unrated observations<div class="number">{summary["unrated_observations"]:,}</div></div></div>
    <p class="note">This is a screening tool for published satellite detections. A grid cell is not a verified facility.
    Rates describe individual observations; they are not annual emissions. No detection can also mean no suitable observation.</p>
    <section id="investigations"><h2>Evidence-backed investigation cases (up to five)</h2>
    <p class="muted">We select up to five cells with observations on at least two distinct
    <em>eligible</em> UTC dates, ranked by eligible dates then peak reported rate.
    We space cases at least 10 km apart to avoid showing adjacent cells as separate leads.
    These are geographic screening leads, not verified leaking facilities. Source quality
    ratings marked not supplied remain provisional.</p>
    <p><a href="investigation_summary.csv">Download case summary</a> ·
    <a href="investigations.json">Download all case dates and criteria (JSON)</a>.</p>
    {case_html}</section>
    <section><h2>World map and geographic context</h2><p class="muted">Showing {len(plotted):,} of {len(locations):,} occupied H3 cells at resolution {cfg.h3_resolution}.
    Amber means detections on at least two eligible dates; teal means a single eligible date; gray has no eligible rate.
    Zoom in on a numbered cluster to inspect individual cells. The world outline comes from Natural Earth. All cells are included in the exports.</p>
    <iframe title="Satellite observation map" srcdoc="{map_src}"></iframe></section>
    <section>{chart1}<p class="muted">Changes in counts can reflect coverage, publication delays, and sensor deployment. These counts do not establish emissions trends.</p></section>
    <section>{chart2}</section><section><h2>Review queue</h2>
    <p>Repeated eligible detection dates come first, then the largest observed rate. Unknown quality ratings remain marked;
    questionable and bad ratings are excluded from rate-based screening. This order is a transparent triage rule.</p>
    <div class="table-scroll">{table_html}</div></section>
    <section><h2>How to interpret this report</h2><p>Observation range: {html.escape(summary["earliest_observation"])} to {html.escape(summary["latest_observation"])}.
    Missing rate values: {summary["missing_rate_observations"]:,}; missing uncertainty values: {summary["missing_uncertainty_observations"]:,}. The full export retains uncertainty, quality flags, instrument, processing version, and raw-page references.</p>
    <p>H3 boundaries can split one site or group several sites. Distinct detection dates are not a detection-frequency estimate without the denominator of all usable overpasses.</p>
    <p>Data provided by Carbon Mapper, Inc. Data and derivatives remain subject to its
    <a href="https://carbonmapper.org/terms">non-commercial terms and attribution requirements</a>.
    Processing here includes filtering, normalization, quality masking, H3 grouping, and SQL aggregation. This project is independently developed and is not endorsed by Carbon Mapper.</p>
    <p class="muted">Run {html.escape(run_id)} · Data contract {CONTRACT_VERSION}</p></section></main></body></html>"""
    # Publish HTML last so a failed export leaves the previous self-contained report readable.
    temporary = directory / "report.html.tmp"
    temporary.write_text(document, encoding="utf-8")
    temporary.replace(directory / "report.html")
    return directory, summary
