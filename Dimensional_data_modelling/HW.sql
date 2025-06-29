-- Business Case Scenario:
-- This script models the actor_films dataset to facilitate efficient analysis of actors' careers and film participation.
-- It defines new tables and data types to store an actor's filmography as arrays of structured records, classifies actors by performance quality, and tracks their active status over time.
-- The script implements both cumulative and SCD Type 2 modeling, enabling historical and incremental tracking of changes in actor performance and activity.
-- This supports advanced analytics on actor career progression, film participation, and performance trends, providing a robust foundation for entertainment industry reporting and business intelligence.

-- 1. DDL for actors table: Create a DDL for an actors table with the following fields:

-- DDL scripts for actors Table
-- Composite type(struct) to store film statistics for a actor

CREATE TYPE films AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

-- ENUM type to classify a actors performance in a film

CREATE TYPE quality_class AS ENUM(
	'star',
	'good',
	'average',
	'bad'
);

-- Actors Table
CREATE TABLE actors (
	actor TEXT,
	current_year INTEGER,
	films films[],
	quality_class quality_class,
	is_active BOOLEAN,
	PRIMARY KEY(actor, current_year)
);

-- 2. Cumulative table generation query: Write a query that populates the actors table one year at a time.

INSERT INTO actors
WITH last_year AS (
	SELECT * FROM actors
	WHERE current_year = 1969
), this_year AS (
	SELECT * FROM actor_films
	WHERE year = 1970
), joined AS (
	SELECT
		COALESCE(t.actor, l.actor) AS actor,
		COALESCE(t.year, l.current_year + 1) AS current_year,
		t.film,
		t.votes,
		t.rating,
		t.filmid,
		l.films AS previous_films,
		t.year IS NOT NULL AS is_active
	FROM this_year t
	FULL OUTER JOIN last_year l
		ON t.actor = l.actor
)
SELECT
	actor,
	current_year,
	COALESCE(previous_films, '{}') || ARRAY_AGG(
		ROW(film, votes, rating, filmid)::films
	) FILTER (WHERE film IS NOT NULL) AS films,
	CASE
		WHEN AVG(rating) > 8 THEN 'star'
		WHEN AVG(rating) > 7 THEN 'good'
		WHEN AVG(rating) > 6 THEN 'average'
		ELSE 'bad'
	END::quality_class,
	BOOL_OR(is_active) AS is_active
FROM joined
GROUP BY actor, current_year, previous_films;

-- Backfill Build

INSERT INTO actors (
    WITH seasons AS (
        SELECT generate_series(1970, 2021) AS year
    ),
    actor_first_year AS (
        SELECT actor, MIN(year) AS first_year
        FROM actor_films
        GROUP BY actor
    ),
    actor_and_years AS (
        SELECT *
        FROM actor_first_year a
        JOIN seasons s ON a.first_year <= s.year
    ),
    joined_data AS (
        SELECT
            ay.actor,
            ay.year AS current_year,
            ARRAY_REMOVE(
                ARRAY_AGG(
                    CASE
                        WHEN af.year IS NOT NULL THEN ROW(
                            af.film,
                            af.votes,
                            af.rating,
                            af.filmid
                        )::films
                    END
                ) OVER (
                    PARTITION BY ay.actor
                    ORDER BY ay.year
                ),
                NULL
            ) AS films_array,
            af.rating,
            af.year IS NOT NULL AS is_active
        FROM actor_and_years ay
        LEFT JOIN actor_films af
            ON ay.actor = af.actor AND ay.year = af.year
    )
    SELECT
        actor,
        current_year,
        films_array AS films,
        CASE
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 THEN 'good'
            WHEN AVG(rating) > 6 THEN 'average'
            ELSE 'bad'
        END::quality_class AS quality_class,
        is_active
    FROM joined_data
    GROUP BY actor, current_year, films_array, is_active
);

-- 3. DDL for actors_history_scd table: Create a DDL for an actors_history_scd table with the following features:

CREATE TABLE actors_history_scd (
	actor TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER,
	current_year INTEGER,
	PRIMARY KEY(actor, start_date)
);

-- 4. Backfill query for actors_history_scd: 
-- Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

INSERT INTO actors_history_scd
	WITH with_previous_stats AS(
		SELECT 
			actor, 
			current_year,
			quality_class,
			LAG(quality_class, 1) OVER(PARTITION BY actor ORDER BY current_year) AS previous_quality_class,
			is_active,
			LAG(is_active, 1) OVER(PARTITION BY actor ORDER BY current_year) AS previous_is_active
		FROM actors
		WHERE current_year <= 2020
	), with_change_indicator AS(
		SELECT
			*,
			CASE
				WHEN quality_class <> previous_quality_class THEN 1
				WHEN is_active <> previous_is_active THEN 1
				ELSE 0
			END AS change_indicator
		FROM with_previous_stats
	), with_streaks AS(
		SELECT 
			*,
			SUM(change_indicator) OVER(PARTITION BY actor ORDER BY current_year) AS streak_indicator
		FROM with_change_indicator
	)
	SELECT 
		actor,
		quality_class,
		is_active,
		MIN(current_year) AS start_date,
		MAX(current_year) AS end_date,
		2020 AS current_year
	FROM with_streaks
	GROUP BY actor, is_active, quality_class, streak_indicator
	ORDER BY actor, streak_indicator;

-- 5. Incremental query for actors_history_scd: 
-- Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.

CREATE TYPE actors_scd_type AS (
	quality_class quality_class,
	is_active boolean,
	start_date INTEGER,
	end_date INTEGER
);

WITH last_date_scd AS(
	SELECT * FROM actors_history_scd
	WHERE current_year = 2020 AND 
		end_date = 2020
), historical_scd AS(
	SELECT 
		actor,
		quality_class,
		is_active,
		start_date,
		end_date
	FROM actors_history_scd
	WHERE current_year = 2020 AND 
		end_date < 2020
), today_date_data AS(
	SELECT * FROM actors
	WHERE current_year = 2021
), unchanged_records AS(
	SELECT
		td.actor,
		td.quality_class,
		td.is_active,
		ls.start_date,
		td.current_year AS end_date
	FROM today_date_data td
	JOIN last_date_scd ls
		ON ls.actor = td.actor
	WHERE td.is_active = ls.is_active AND
		td.quality_class = ls.quality_class
),changed_records AS(
	SELECT
		td.actor,
		UNNEST(ARRAY[
			ROW(
				ls.quality_class,
				ls.is_active,
				ls.start_date,
				ls.end_date
				)::actors_scd_type,
			ROW(
				td.quality_class,
				td.is_active,
				td.current_year,
				td.current_year
				)::actors_scd_type
		]) as records
	FROM today_date_data td
	LEFT JOIN last_date_scd ls
		ON ls.actor = td.actor
	WHERE (td.quality_class <> ls.quality_class
		OR td.is_active <> ls.is_active)
), unnested_changed_records AS(
	SELECT
		actor,
		(records::actors_scd_type).quality_class,
		(records::actors_scd_type).is_active,
		(records::actors_scd_type).start_date,
		(records::actors_scd_type).end_date
	FROM changed_records
), new_records AS(
	SELECT
		td.actor,
		td.quality_class,
		td.is_active,
		td.current_year AS start_date,
		td.current_year AS end_date
	FROM today_date_data td
	LEFT JOIN last_date_scd ls
		ON td.actor = ls.actor
	WHERE ls.actor IS NULL
)
SELECT
	*,
	2022 AS current_year
FROM (
	SELECT *
	FROM historical_scd
	
	UNION ALL
	
	SELECT *
	FROM unchanged_records
	
	UNION ALL
	
	SELECT *
	FROM unnested_changed_records
	
	UNION ALL
	
	SELECT *
	FROM new_records
) a