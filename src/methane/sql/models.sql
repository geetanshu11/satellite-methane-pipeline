
CREATE OR REPLACE TABLE dim_platform AS
SELECT DISTINCT instrument, platform FROM fact_observations;

CREATE OR REPLACE TABLE dim_sector AS
SELECT DISTINCT f.sector_code, coalesce(s.sector_name, 'Unmapped: ' || f.sector_code) AS sector_name
FROM fact_observations f LEFT JOIN sector_lookup s USING (sector_code);

CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT CAST(observed_at AS DATE) AS observation_date,
       year(observed_at)::INTEGER AS year, month(observed_at)::INTEGER AS month,
       dayofweek(observed_at)::INTEGER AS weekday
FROM fact_observations;

CREATE OR REPLACE TABLE mart_monthly AS
SELECT date_trunc('month', observed_at)::DATE AS observation_month,
       instrument, count(*) AS observations, count(DISTINCT scene_id) AS scenes,
       count(DISTINCT h3_cell) AS occupied_cells,
       count(*) FILTER (WHERE review_eligible) AS review_eligible_observations
FROM fact_observations GROUP BY 1, 2;

CREATE OR REPLACE TABLE mart_sectors AS
SELECT f.sector_code, s.sector_name, count(*) AS observations,
       count(DISTINCT f.scene_id) AS scenes, count(DISTINCT f.h3_cell) AS occupied_cells,
       median(f.emission_rate_kg_hr) FILTER (WHERE f.review_eligible) AS median_observed_rate_kg_hr,
       count(*) FILTER (WHERE f.plume_quality='not_supplied') AS unrated_observations
FROM fact_observations f JOIN dim_sector s USING(sector_code) GROUP BY 1,2;

CREATE OR REPLACE TABLE mart_locations AS
SELECT h3_cell, count(*) AS observations, count(DISTINCT scene_id) AS scenes,
       count(DISTINCT CAST(observed_at AS DATE)) AS observation_days,
       count(DISTINCT CAST(observed_at AS DATE)) FILTER (WHERE review_eligible) AS eligible_observation_days,
       min(observed_at) AS first_observed_at, max(observed_at) AS last_observed_at,
       max(emission_rate_kg_hr) FILTER (WHERE review_eligible) AS peak_observed_rate_kg_hr,
       median(emission_rate_kg_hr) FILTER (WHERE review_eligible) AS median_observed_rate_kg_hr,
       arg_max(plume_id, emission_rate_kg_hr) FILTER (WHERE review_eligible) AS peak_plume_id,
       count(*) FILTER (WHERE plume_quality='not_supplied') AS unrated_observations,
       string_agg(DISTINCT sector_code, ', ' ORDER BY sector_code) AS sector_codes
FROM fact_observations GROUP BY h3_cell;

CREATE OR REPLACE TABLE mart_review_queue AS
SELECT row_number() OVER (ORDER BY (eligible_observation_days >= 2) DESC,
                                  peak_observed_rate_kg_hr DESC, h3_cell) AS review_order,
       *, eligible_observation_days >= 2 AS repeated_detection_cell
FROM mart_locations WHERE peak_observed_rate_kg_hr IS NOT NULL;

-- LAG shows changes in distinct encountered revisions; this is not source-valid SCD2.
CREATE OR REPLACE VIEW mart_revision_history AS
SELECT observation_id, record_hash, modified_at, first_seen_at, first_seen_run,
       emission_rate_kg_hr,
       emission_rate_kg_hr - lag(emission_rate_kg_hr) OVER (
          PARTITION BY observation_id ORDER BY modified_at, first_seen_at, record_hash) AS rate_change_kg_hr,
       row_number() OVER (PARTITION BY observation_id ORDER BY modified_at, first_seen_at, record_hash) AS encountered_revision
FROM observation_versions;
