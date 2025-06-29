-- Business Case Scenario:
-- This script creates a fact table to track user activity dates and encodes activity history using bit arrays for efficient storage and querying.
-- It enables fast analysis of user engagement patterns, retention, and activity streaks at daily, weekly, or monthly granularity.

-- DDL for users_cumulated table

DROP TABLE users_cumulated;

CREATE TABLE users_cumulated (
	user_id TEXT, 
	-- The list of dates in the past where the suer was active
	dates_active DATE[],
	-- the current data for the user
	date DATE, 
	PRIMARY KEY(user_id, date)
);


-- Cumulative query for users_cumulated table

INSERT INTO users_cumulated
	WITH yesterday AS (
		SELECT * 
		FROM users_cumulated
		WHERE date = DATE('2022-12-31')
	), today AS (
		SELECT
			CAST(user_id AS TEXT) AS user_id,
			DATE(CAST(event_time AS TIMESTAMP)) AS date_active
		FROM events
		WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
			AND user_id IS NOT NULL
		GROUP BY 1, 2
	 )
	SELECT
		COALESCE (t.user_id, y.user_id) AS user_id,
		CASE 
			WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
			WHEN t.date_active IS NULL THEN y.dates_active
			-- Most recent date is appeneded at lower indexes of array
			ELSE ARRAY[t.date_active] || y.dates_active 
		END AS dates_active,
		COALESCE (t.date_active, y.date + INTERVAL '1 day') AS date
	FROM today t
	FULL OUTER JOIN yesterday y
		ON t.user_id = y.user_id;



-- Using bit create a datelist that will track the monthly activity of a user, can be queried to weekly or daily
WITH starter AS (
	SELECT 
		uc.dates_active @> ARRAY [DATE(d.valid_date)] AS is_active,
		EXTRACT(DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since,
		uc.user_id,
		d.valid_date,
		uc.dates_active
	FROM users_cumulated uc
		CROSS JOIN(
			SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date
		) as d
	WHERE date = DATE('2023-01-31')
),bits AS (
	SELECT user_id,
		SUM(CASE
			WHEN is_active THEN POW(2, 32 - days_since)
			ELSE 0 END)::bigint::bit(32) AS datelist_int,
		DATE('2023-01-31') as date
	FROM starter
	GROUP BY user_id
)
SELECT * FROM bits ORDER BY datelist_int;

-- scf
-- SELECT * FROM starter
-- WHERE user_id = '2780254311411550000';