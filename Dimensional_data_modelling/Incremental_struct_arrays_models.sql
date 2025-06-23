-- DImensional Data Model with Structs and Arrays
-- Data used in this model is from the NBA seasons, /data/palyer_seasons.csv
-- Select the first 100 rows from the player_seasons table
SELECT * 
FROM public.player_seasons
LIMIT 100;

-- Create a composite type(struct) to store season statistics for a player
CREATE TYPE season_stats AS (
	season INTEGER,
	gp INTEGER,
	pts REAL, 
	reb REAL, 
	ast REAL
);

-- Create an enum type to classify a player's scoring ability
CREATE TYPE current_scoring_class AS ENUM(
	'star',
	'good',
	'average',
	'bad'
);

-- Create the players table to store player information and their season stats, these dimensions do not change
CREATE TABLE players (
	player_name TEXT,
	height TEXT,
	college TEXT, 
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[], -- using an array of the composite type
	current_scoring_class current_scoring_class,
	years_since_last_session INTEGER,
	current_season INTEGER,
	is_active BOOLEAN,
	PRIMARY KEY(player_name, current_season)
);

-- Find the earliest season in the player_seasons table
SELECT 
	MIN(season)
FROM player_seasons;

-- (Incremental Build) This is a cumulative query
-- Insert a new set of players for the next season, combining data from the previous season and the current one
-- This acts as a seed query for the players table
INSERT INTO players
	WITH yesterday AS (
		SELECT * FROM players
		WHERE current_season = 2001
	),
	today AS (
		SELECT * FROM player_seasons
		WHERE season = 2002
	)
	SELECT
		COALESCE(t.player_name, y.player_name) AS player_name,
		COALESCE(t.height, y.height) AS height,
		COALESCE(t.college, y.college) AS college,
		COALESCE(t.country, y.country) AS country,
		COALESCE(t.draft_year, y.draft_year) AS draft_year,
		COALESCE(t.draft_round, y.draft_round) AS draft_round,
		COALESCE(t.draft_number, y.draft_number) AS draft_number,
		CASE 
			WHEN y.season_stats IS NULL
				THEN ARRAY[
					ROW(
						t.season,
						t.gp,
						t.pts,
						t.reb,
						t.ast
					)::season_stats
				]
			WHEN t.season IS NOT NULL
				THEN y.season_stats || ARRAY[
					ROW(
						t.season,
						t.gp,
						t.pts,
						t.reb,
						t.ast
					)::season_stats
				]
			ELSE y.season_stats
		END AS season_stats,
		CASE
			WHEN t.season IS NOT NULL THEN 
				CASE
					WHEN t.pts > 20 THEN 'star'
					WHEN t.pts > 15 THEN 'good'
					WHEN t.pts > 10 THEN 'average'
					ELSE 'bad'
				END::current_scoring_class
			ELSE y.current_scoring_class
		END as current_scoring_class,
		CASE 
		  WHEN t.season IS NOT NULL THEN 0
		  ELSE y.years_since_last_session + 1
		END as years_since_last_session,
		COALESCE(t.season, y.current_season + 1) AS current_season,
		t.season IS NOT NULL as is_active
	FROM today t 
	FULL OUTER JOIN yesterday y 
	ON t.player_name = y.player_name;

-- Select players who have not played for more than 4 years
SELECT
	player_name, 
	current_scoring_class,
	years_since_last_session,
	season_stats,
	current_season
FROM players
WHERE years_since_last_session > 4;

-- Compare the first and latest season stats for 'star' players and calculate performance ratio
SELECT
	player_name,
	(season_stats[1]::season_stats).pts AS old_stat_pts,
	(season_stats[1]::season_stats).season AS old_stat_season,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts AS latest_stat_pts,
	(season_stats[CARDINALITY(season_stats)]::season_stats).season AS latest_stat_season,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts /
		CASE 
			WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
			ELSE (season_stats[1]::season_stats).pts
		END AS pts_performance
FROM players
WHERE current_scoring_class = 'star'
ORDER BY pts_performance DESC;

-- Unnest the season_stats array for Michael Jordan in the 2001 season
SELECT
	player_name,
	UNNEST(season_stats) AS season_stats
FROM players 
WHERE current_season = 2001 AND player_name='Michael Jordan';

-- Unnest the season_stats array and expand the composite type for Michael Jordan in the 2001 season
SELECT
	player_name,
	(UNNEST(season_stats)::season_stats).* AS season_stats
FROM players 
WHERE current_season = 2001 AND player_name='Michael Jordan';

-- Drop the players table (cleanup)
DROP TABLE players;

-- (Backfill Build) Direct insert data from the first season to last season into the players table
INSERT INTO players
WITH years AS (
    SELECT * FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL THEN ROW(
                        ps.season,
                        ps.gp,
                        ps.pts,
                        ps.reb,
                        ps.ast
                    )::season_stats
                END
            ) OVER (
                PARTITION BY pas.player_name
                ORDER BY COALESCE(pas.season, ps.season)
            ),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    w.seasons AS season_stats,    
    CASE
        WHEN (w.seasons[CARDINALITY(w.seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (w.seasons[CARDINALITY(w.seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (w.seasons[CARDINALITY(w.seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::current_scoring_class AS current_scoring_class,
    w.season - (w.seasons[CARDINALITY(w.seasons)]::season_stats).season AS years_since_last_session,
    w.season AS current_season,
    (w.seasons[CARDINALITY(w.seasons)]::season_stats).season = w.season AS is_active
FROM windowed w
JOIN static s ON w.player_name = s.player_name;
