-- DDL for user_devices_cumulated table
DROP TABLE user_devices_cumulated;

CREATE TABLE user_devices_cumulated (
	user_id TEXT,
	date DATE,
	browser_type TEXT,
	device_activity_datelist DATE[],
	PRIMARY KEY(user_id, browser_type, date)
);

-- Deduplicating the events table
-- A cumulative query to generate device_activity_datelist from events table and insert it into the user_devices_cumulated table

INSERT INTO user_devices_cumulated
	WITH deduped AS (
		SELECT 
			ROW_NUMBER() OVER(PARTITION BY d.device_id, e.user_id, e.event_time) AS row_no,
			CAST(e.user_id AS TEXT) AS user_id,
			d.device_id, 
			d.browser_type,
			e.event_time
		FROM devices d
		FULL OUTER JOIN events e
			ON d.device_id = e.device_id
		WHERE e.user_id IS NOT NULL
	), yesterday AS(
		SELECT * FROM user_devices_cumulated
		WHERE date = DATE('2023-01-30') 
	), today AS(
		SELECT
			user_id,
			browser_type,
			DATE(CAST(event_time AS TIMESTAMP)) AS date_active
		FROM deduped
		WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
			AND row_no = 1
		GROUP BY 1, 2, 3
	)
	SELECT
		COALESCE(t.user_id, y.user_id) AS user_id,
		COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date,
		COALESCE(t.browser_type, y.browser_type, 'NA') AS browser_type,
		CASE
			-- If nothing from yesterday, start a new array
			WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
			
			-- If today has no new activity, carry over yesterday's array
			WHEN t.date_active IS NULL THEN y.device_activity_datelist
				
			-- Else: append today's data to the existing array
			ELSE 
				y.device_activity_datelist || ARRAY[t.date_active]
		END AS device_activity_datelist
	FROM yesterday y
	FULL OUTER JOIN today t
		ON y.user_id = t.user_id AND y.browser_type = t.browser_type;


-- A datelist_int generation query. Converting the device_activity_datelist column into a datelist_int column

WITH starter AS (
	SELECT
		uc.device_activity_datelist @> ARRAY[DATE(gd.valid_date)] AS is_active,
		EXTRACT(DAY FROM DATE('2023-01-31') - gd.valid_date) AS days_since,
		uc.user_id, 
		gd.valid_date,
		uc.device_activity_datelist
	FROM user_devices_cumulated uc
	CROSS JOIN(
		SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date
	) AS gd
	WHERE date = DATE('2023-01-31')
),bits AS (
	SELECT 
		user_id,
		SUM(CASE
				WHEN is_active THEN POW(2, 32 - days_since)
				ELSE 0 
			END)::bigint::bit(32) AS datelist_int,
		DATE('2023-01-31') as date
	FROM starter
	GROUP BY user_id
)
SELECT * FROM bits ORDER BY datelist_int;