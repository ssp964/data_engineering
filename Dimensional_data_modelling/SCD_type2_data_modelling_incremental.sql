-- Business Case Scenario:
-- This script implements a Slowly Changing Dimension (SCD) Type 2 model for tracking changes in player scoring class and activity status over time.
-- It maintains a full history of changes, enabling historical analysis of player performance, career transitions, and status changes.
-- This supports accurate reporting, longitudinal studies, and compliance with data warehousing best practices for sports analytics and business intelligence.

-- Modelling SCD type 2 table

SELECT 
	player_name,
	current_scoring_class,
	is_active
FROM players;

-- Analyse the scoring performance throught out the career seasons respective to the player
CREATE TABLE players_scd(
	player_name TEXT,
	scoring_class current_scoring_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, start_season)
); 

-- (Backfill Build) Creating indicators and tracking the scoring class and seasons when player are active. Then insert into player_scd
INSERT INTO players_scd (
	WITH with_previous_performance AS (
		SELECT
			player_name,
			current_season,
			current_scoring_class,
			LAG(current_scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class, --tracks the previous scoring class
			is_active,
			LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active --tracks the previous is_active
		FROM players
		WHERE current_season <= 2021
	), with_change_indicator AS(
		SELECT 
			*,
			CASE
				WHEN current_scoring_class <> previous_scoring_class THEN 1
				WHEN is_active <> previous_is_active THEN 1
				ELSE 0
			END AS change_indicator
		FROM with_previous_performance
	), with_streaks AS(
		SELECT
			*,
			SUM(change_indicator) OVER(PARTITION BY player_name ORDER BY current_season) AS streak_indicator
		FROM with_change_indicator
	)
	SELECT
		player_name,
		current_scoring_class AS scoring_class,
		is_active,
		MIN(current_season) AS start_season,
		MAX(current_season) AS end_season,
		2021 AS current_season
	FROM with_streaks
	GROUP BY player_name, is_active, streak_indicator, current_scoring_class
	ORDER BY player_name, streak_indicator
)

SELECT 
	* 
FROM players_scd


-- (Incremental Build) Efficient way to insert into player_scd compared to generating years and then incrementing the data

-- Create a composite type for the SCD type 2 table
CREATE TYPE scd_type AS (
	scoring_class current_scoring_class,
	is_active boolean,
	start_season INTEGER,
	end_season INTEGER
)

-- Scd generation
WITH last_season_scd AS (
	SELECT 
		*
	FROM players_scd
	WHERE 
		current_season = 2021 AND
		end_season = 2021
), historical_scd AS(
	SELECT 
		player_name,
		scoring_class,
		is_active,
		start_season,
		end_season
	FROM players_scd
	WHERE 
		current_season = 2021 AND
		end_season < 2021
),this_season_data AS (
	SELECT 
		*
	FROM players
	WHERE current_season = 2022
), unchanged_records AS (
		SELECT
			ts.player_name,
			ts.current_scoring_class,
			ts.is_active,
			ls.start_season,
			ts.current_season as end_season
        FROM this_season_data ts
        JOIN last_season_scd ls
        	ON ls.player_name = ts.player_name
		WHERE ts.current_scoring_class = ls.scoring_class
			AND ts.is_active = ls.is_active
	),
	changed_records AS (
        SELECT
                ts.player_name,
                UNNEST(ARRAY[
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_season
                        )::scd_type,
                    ROW(
                        ts.current_scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                        )::scd_type
                ]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        	ON ls.player_name = ts.player_name
		WHERE (ts.current_scoring_class <> ls.scoring_class
			OR ts.is_active <> ls.is_active)
	),
	unnested_changed_records AS (
		SELECT player_name,
			(records::scd_type).scoring_class,
			(records::scd_type).is_active,
			(records::scd_type).start_season,
			(records::scd_type).end_season
		FROM changed_records
		),
	new_records AS (
		SELECT
			ts.player_name,
			ts.current_scoring_class,
			ts.is_active,
			ts.current_season AS start_season,
			ts.current_season AS end_season
		FROM this_season_data ts
		LEFT JOIN last_season_scd ls
			ON ts.player_name = ls.player_name
		WHERE ls.player_name IS NULL
     )
SELECT 
	*, 
	2022 AS current_season 
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

