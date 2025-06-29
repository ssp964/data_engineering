-- Business Case Scenario:
-- This script aggregates user metrics (such as site hits) into arrays for each month, allowing for efficient storage and retrieval of time-series data.
-- It supports advanced analytics on user behavior, monthly trends, and metric aggregation, enabling business intelligence and reporting on user engagement and activity patterns.

-- DDL for array_metrics table

DROP TABLE array_metrics;

CREATE TABLE array_metrics(
	user_id NUMERIC,
	month_start DATE,
	metric_name TEXT,
	metric_array REAL[],
	PRIMARY KEY(user_id, month_start, metric_name)
);

-- Inserting data into array_metrics table

INSERT INTO array_metrics
	WITH daily_aggregate AS (
		SELECT
			user_id,
			DATE(event_time) AS date,
			COUNT(1) AS num_site_hits
		FROM events
		WHERE DATE(event_time) = DATE('2023-01-03')
			AND user_id IS NOT NULL
		GROUP BY user_id, DATE(event_time)
	), yesterday_array AS (
		SELECT *
		FROM array_metrics
		WHERE month_start = DATE('2023-01-01')
	)
	SELECT
		COALESCE(da.user_id, ya.user_id) AS user_id,
		COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
		'site_hits' AS metric_name,
        -- calulcates the site hits for the day and adds it to the array
		CASE
			WHEN ya.metric_array IS NOT NULL 
				THEN ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
            -- also handles the case where the hits are not present for the day, by adding 0 to the array
			WHEN ya.metric_array IS NULL
				THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(da.num_site_hits, 0)]
		END AS metric_array
	FROM daily_aggregate da
	FULL OUTER JOIN yesterday_array ya
		ON da.user_id = ya.user_id
	ON CONFLICT (user_id, month_start, metric_name)
	DO
		UPDATE SET metric_array = EXCLUDED.metric_array;

SELECT * FROM array_metrics;

-- Calculate the monthly site hits of users on the website

WITH agg_hits AS (
	SELECT 
		metric_name, 
		Month_start,
		ARRAY[
			SUM(metric_array[1]),
			SUM(metric_array[2]),
			SUM(metric_array[3])
		] AS summed_array
	FROM array_metrics
	GROUP BY 1, 2
)
SELECT
	metric_name,
	-- As the index value in postgres starts from 1 and We want to increment a month for displaying monthly value
	month_start + CAST(CAST(index - 1 AS TEXT) || 'day' AS INTERVAL) AS month_date,
	elem AS hits
FROM agg_hits agg
-- Unnest is used outside of the CTE so that the aggregation is done onse the sum of hits is calculated
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, INDEX);