-- DDL for host_activity_reduced table

DROP TABLE host_activity_reduced;

CREATE TABLE host_activity_reduced(
    month DATE,
	host TEXT,
	hit_array INTEGER[],
    unique_visitors INTEGER[],
	PRIMARY KEY(host, month)
);

-- Incremental day-by-day load for host_activity_reduced table

INSERT INTO host_activity_reduced
WITH yesterday AS (
	SELECT * FROM host_activity_reduced
	WHERE month = DATE_TRUNC('month', DATE '2023-01-04')
), today AS (
	SELECT
	  host,
	  DATE_TRUNC('month', DATE(event_time)) AS month,
	  COUNT(*) AS hit_array_count,
	  COUNT(DISTINCT user_id) AS unique_visitors_count
	FROM events
	WHERE DATE(event_time) = DATE '2023-01-05'
	  AND user_id IS NOT NULL
	GROUP BY host, month
)
SELECT
	COALESCE(t.month, y.month) AS month,
	COALESCE(t.host, y.host) AS host,
	CASE
		WHEN y.hit_array IS NULL THEN ARRAY[t.hit_array_count]
		WHEN t.hit_array_count IS NULL THEN y.hit_array
		ELSE y.hit_array || ARRAY[t.hit_array_count]
	END AS hit_array,
	CASE
		WHEN y.unique_visitors IS NULL THEN ARRAY[t.unique_visitors_count]
		WHEN t.unique_visitors_count IS NULL THEN y.unique_visitors
		ELSE y.unique_visitors || ARRAY[t.unique_visitors_count]
	END AS unique_visitors
FROM yesterday y
FULL OUTER JOIN today t
	ON y.host = t.host

ON CONFLICT (host, month)
DO UPDATE SET
	hit_array = EXCLUDED.hit_array,
	unique_visitors = EXCLUDED.unique_visitors;

-- Check the host_activity_reduced table

SELECT * FROM host_activity_reduced;


