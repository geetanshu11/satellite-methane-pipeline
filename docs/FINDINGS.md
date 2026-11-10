# What the catalogue contains

The historical load collected **21,329 published methane observations** from EMIT and Tanager worldwide. It covered 8,213 distinct scenes. The input cutoff was `2026-11-05T16:51:44.059908Z`. The API returned 21,329 observations for the same two instruments without a date filter. All instruments together returned 37,982 published methane observations. [Count queries](../source_manifests/catalog_counts.json) · [Archive hashes](../source_manifests/historical.json)

| Coverage measure | Verified result |
|---|---:|
| Observation dates in UTC | 2022-08-10 through 2026-10-06 |
| EMIT / Tanager observations | 4,311 / 17,018 |
| Source latitude range | -52.86° to 71.57° |
| Source longitude range | -149.61° to 176.78° |
| Occupied H3 cells at resolution 8 | 7,226 |
| Cells with eligible observations on two or more UTC dates | 2,814 |
| Missing provider plume quality rating | 21,329 |
| Unavailable modeled rate / uncertainty | 4,479 / 4,479 |

**Sector.** The three largest provider-attributed categories were oil and natural gas (9,594 observations), solid waste disposal (6,602), and coal mining (4,488). They describe the composition of the published observations. Estimating the emissions share of each sector would require additional coverage information.

**Location.** The most frequently represented cell, `882cc56cbdfffff`, is centered at 37.927°, 53.922°. It contains 48 observations on 25 distinct dates, with an eligible rate on 18 dates. Several sites can occupy a cell. The observations support follow-up on those dates, while facility identity and emissions between visits remain unknown.

**Time.** Annual observation counts were 2022 (142), 2023 (1,697), 2024 (2,154), 2025 (9,654), 2026 (7,682). The first and last years cover partial periods. Sensor deployment, available overpasses and publication timing affect these counts.

**Missingness.** 16,850 observations had a positive visible rate eligible for the review queue. Missing ratings remain provisional. Rates describe individual observations in kg/hr. The catalogue alone cannot establish annual emissions, regulatory violations or verified reductions.

## Reproducing the result

The two frozen archives contain a full capture from November 5, 2026 (22 pages, 21,329 observations) and an overlapping incremental capture (129 records). This refactor replayed both archives offline, then repeated the delta. Current observations and distinct versions stayed at **21,329**; the repeated delta added, updated and retired **zero** records.

The original and simplified implementations were each run against the same archives in separate databases. **All 24 fact columns and every UUID/version-hash pair matched.** Page checksums and archive manifest hashes were verified. The SQL queries below were executed against the resulting warehouse. Both charts and the map are generated from these tables.

A separate live check on November 9, 2026 used the small California/2025 scope: **42 observations in two API requests**, followed by an incremental request returning **zero records** and no changes. The worldwide historical snapshot above was validated through offline replay; this live check does not refresh its counts. Revision, withdrawal and failure scenarios use explicitly synthetic test fixtures.

Use the default historical replay in the [README](../README.md) and run these queries in DuckDB.

```sql
SELECT sector_name, observations FROM mart_sectors ORDER BY observations DESC;
SELECT count(*) AS occupied_cells,
       count(*) FILTER (WHERE eligible_observation_days >= 2) AS repeated_cells
FROM mart_locations;
SELECT instrument, count(*), min(observed_at), max(observed_at)
FROM fact_observations GROUP BY instrument;
```

Data provided by Carbon Mapper, Inc. [Data terms and attribution](../DATA_NOTICE.md)
