--DDL for hosts_cumulated table

DROP TABLE hosts_cumulated;

CREATE TABLE hosts_cumulated(
	host TEXT,
	date DATE,
	host_activity_datelist DATE[],
	PRIMARY KEY(host, date)
);

-- Cumulative query to generate host_activity_datelist from events table and insert it into the hosts_cumulated table

INSERT INTO hosts_cumulated
	WITH yesterday AS (
		SELECT * FROM hosts_cumulated
		WHERE date = DATE('2023-01-03')
	), today AS (
		SELECT
			host, 
			DATE(event_time) AS date_active
		FROM events
		WHERE DATE(event_time) = DATE('2023-01-04')
		GROUP BY 1, 2
	)
	SELECT
		COALESCE(t.host, y.host) AS host,
		COALESCE(t.date_active, y.date) AS date,
		CASE
			WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]
			WHEN t.date_active IS NULL THEN y.host_activity_datelist
			ELSE y.host_activity_datelist || ARRAY[t.date_active]
		END AS host_activity_datelist
	FROM yesterday y
	FULL OUTER JOIN today t
		ON y.host = t.host;

SELECT * FROM hosts_cumulated;