-- Using UTC keeps these results aligned with the notebook's observation dates.
SET TimeZone='UTC';

-- Looking at the last runs helps me separate pipeline health from source measurements.
SELECT run_id, mode, status, started_at, finished_at, metrics_json, error
FROM pipeline_runs ORDER BY started_at DESC LIMIT 10;

-- These are the warnings and checks recorded for the most recent run with checks.
SELECT q.* FROM quality_results q
JOIN pipeline_runs p USING (run_id)
WHERE q.run_id = (SELECT q2.run_id FROM quality_results q2
                 JOIN pipeline_runs p2 USING (run_id) ORDER BY p2.started_at DESC LIMIT 1)
ORDER BY severity, check_name;

-- Multiple dates in one cell are a reason to investigate, not proof of continuous leakage.
SELECT q.review_order, q.h3_cell, d.center_latitude, d.center_longitude,
       q.observations, q.eligible_observation_days,
       q.peak_observed_rate_kg_hr, q.unrated_observations
FROM mart_review_queue q JOIN dim_location d USING (h3_cell)
ORDER BY review_order LIMIT 25;

-- Keeping uncertainty alongside each rate makes the individual observations easier to interpret.
SELECT plume_id, observed_at, platform, sector_code,
       emission_rate_kg_hr, uncertainty_kg_hr, plume_quality, raw_page
FROM fact_observations WHERE review_eligible
ORDER BY emission_rate_kg_hr DESC, plume_id LIMIT 20;

-- Coverage and publication patterns can affect these counts.
SELECT * FROM mart_monthly ORDER BY observation_month, instrument;

-- Missing ratings should remain visible when describing the dataset.
SELECT plume_quality, count(*) AS observations,
       count(*) FILTER (WHERE emission_rate_kg_hr IS NULL) AS missing_rates,
       count(*) FILTER (WHERE review_eligible) AS screening_eligible
FROM fact_observations GROUP BY plume_quality ORDER BY observations DESC;

-- Comparing the current fact with encountered history shows how many revisions have accumulated.
SELECT (SELECT count(*) FROM fact_observations) AS current_observations,
       (SELECT count(*) FROM observation_versions) AS encountered_versions,
       (SELECT count(*) FROM retirements) AS recorded_retirements;
