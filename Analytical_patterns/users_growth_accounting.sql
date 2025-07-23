-- Objective:
-- Tracking the user's activity daily and weekly (State Transition Tracking)

-- DDL for users_growth_accounting
DROP TABLE IF EXISTS users_growth_accounting;

CREATE TABLE users_growth_accounting(
    user_id NUMERIC,
    first_date_active DATE,
    last_active_date DATE,
    daily_active_state TEXT,
    weekly_active_state TEXT,
    dates_active DATE[],
    date DATE,
    PRIMARY KEY (user_id, date)
);

-- Cumulation query to update the users_growth_accounting table: 
-- daily_active_state:
-- New: User is active today but has no prior activity history.
-- Retained: User was active yesterday and is also active today.
-- Resurrected: User is active today, was inactive yesterday, but had past activity before.
-- Churned: User was active yesterday but is not active today.
-- Stale: User is not active today and has not been active for a long duration (e.g., 7+ days).

INSERT INTO users_growth_accounting
WITH yesterday AS (
	SELECT 
		*
	FROM users_growth_accounting
	WHERE date = DATE('2022-12-31')
), today AS (
	SELECT
		user_id, 
		DATE_TRUNC('day', event_time::TIMESTAMP) AS today_date,
		COUNT(1)
	FROM events
	WHERE DATE_TRUNC('day', event_time::TIMESTAMP) = DATE('2023-01-01')
	GROUP BY user_id, DATE_TRUNC('day', event_time::TIMESTAMP)
)
SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(y.first_date_active, t.today_date) AS first_date_active,
	COALESCE(t.today_date, y.last_active_date) AS last_active_date,
	CASE
		WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'New'
		WHEN y.last_active_date = t.today_date - INTERVAL '1 day' THEN 'Retained'
		WHEN y.last_active_date < t.today_date - INTERVAL '1 day' THEN 'Resurrected'
		WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
		ELSE 'Stale'
	END AS daily_active_state,
	CASE
		WHEN y.user_id IS NULL THEN 'New'
		WHEN y.last_active_date < t.today_date - Interval '7 day' THEN 'Resurrected'
		WHEN t.today_date IS NULL AND y.last_active_date = y.date - interval '7 day' THEN 'Churned'
		WHEN COALESCE(t.today_date, y.last_active_date) + INTERVAL '7 day' >= y.date THEN 'Retained'
		ELSE 'Stale'
	END AS weekly_active_state,
	COALESCE(t.today_date, y.date + Interval '1 day') as date
FROM today t
FULL OUTER JOIN yesterday y
	ON t.user_id = y.user_id


