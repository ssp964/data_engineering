-- DDL
-- Create a ENUM vertex_type for players
CREATE TYPE vertex_type AS ENUM(
	'player',
	'team',
	'game'
);

-- Vertex table
CREATE TABLE vertices (
	identifier TEXT,
	type vertex_type,
	properties JSON,
	PRIMARY KEY (identifier, type)
);

-- Create Edge Type for the relationship
CREATE TYPE edge_type AS ENUM(
	'plays_against',
	'shares_team',
	'plays_in',
	'plays_on'
);

--Edges Table
CREATE TABLE edges (
	subject_identifier TEXT,
	subject_type vertex_type,
	object_indetifier TEXT,
	object_type vertex_type,
	edge_type edge_type,
	properties JSON,
	PRIMARY KEY (
		subject_identifier,
		subject_type,
		object_indetifier,
		object_type,
		edge_type
	)
);

-- Creating Vertices and Insert into vertex table

-- Inserting 'game' vertex type data into the vertices table
INSERT INTO vertices(
	SELECT
		game_id AS identifier,
		'game'::vertex_type AS type,
		json_build_object(
			'pts_home', pts_home,
			'pts_away', pts_away,
			'winning team',(
				CASE
					WHEN home_team_wins = 1 THEN home_team_id
					ELSE visitor_team_id
				END
			) 
		)AS properties
	FROM games
);

-- Inserting 'player' vertex type data into the vertices table
INSERT INTO vertices(
	WITH player_agg AS(
		SELECT
			player_id AS identifier,
			MAX(player_name) AS player_name,
			COUNT(1) AS number_of_games,
			SUM(pts) AS total_points,
			ARRAY_AGG(DISTINCT team_id) AS teams
		FROM game_details
		GROUP BY player_id
	)
	SELECT
		identifier,
		'player'::vertex_type AS type,
		json_build_object(
			'player_name', player_name,
			'number_of_games',number_of_games,
			'total_points',total_points,
			'teams',teams
		) AS properties
	FROM player_agg
);

-- Inserting 'team' vertex type data into the vertices table
INSERT INTO vertices(
	-- DeDuping the team_id
	WITH deduping_teams AS(
		SELECT 
			ROW_NUMBER() OVER(PARTITION BY team_id) AS row_number_dup,
			*
		FROM teams
	)
	SELECT 
		team_id AS identifier,
		'team'::vertex_type AS type,
		json_build_object(
			'abbreviation',abbreviation,
			'nickname',nickname,
			'city',city,
			'arena',arena,
			'yearfounded',yearfounded
		) AS properties
	FROM deduping_teams
	WHERE row_number_dup = 1
);

-- Creating Edges and Insert into edges table

INSERT INTO edges(
	WITH deduped_game_details AS(
		SELECT
			ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num,
			*
		FROM game_details
	)
	SELECT
		player_id AS subject_identifier,
		'player'::vertex_type AS subject_type,
		game_id AS object_identifier,
		'game'::vertex_type AS object_type,
		'plays_in'::edge_type AS edge_type,
		json_build_object(
			'start_position', start_position,
			'pts', pts,
			'team_id', team_id,
			'team_abbreviation', team_abbreviation
		) AS properties
	FROM deduped_game_details
	WHERE row_num = 1
);


-- Find the players with maximum points

SELECT
  v.properties->>'player_name',                      -- column 1
  MAX(CAST(e.properties->>'pts' AS INTEGER))         -- column 2
FROM vertices v
JOIN edges e
  ON e.subject_identifier = v.identifier
 AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC;

-- Creating a edge
--          [Player]
--        /          \
--       /            \
-- plays_against    shares_team
--     /                \
-- [player]           [player]


-- Creating edges where two players are playing against each other and shares the same team 
-- and insert into edges table
-- ignore the game_id in the end result, used for debugging
INSERT INTO edges (
	WITH deduped AS (
	    SELECT 
			*, 
			row_number() over (PARTITION BY player_id, game_id) AS row_num
	    FROM game_details
	),filtered AS (
		SELECT * FROM deduped
		WHERE row_num = 1
	),aggregated AS (
		SELECT
			f1.player_id AS subject_player_id,
			f2.player_id AS object_player_id,
			CASE 
				WHEN f1.team_abbreviation = f2.team_abbreviation
				THEN 'shares_team'::edge_type
				ELSE 'plays_against'::edge_type
			END AS edge_type,
			MAX(f1.player_name) AS subject_player_name,
			MAX(f2.player_name) AS object_player_name,
			COUNT(1) AS num_games,
			SUM(f1.pts) AS subject_points,
			SUM(f2.pts) as object_points
		FROM filtered f1
			JOIN filtered f2
			ON f1.game_id = f2.game_id
			AND f1.player_name <> f2.player_name
		WHERE f1.player_id > f2.player_id
		GROUP BY
			f1.player_id,
			f2.player_id,
			edge_type
	)
	SELECT 
		subject_player_id AS subject_identifier,
		'player'::vertex_type AS subject_type,
		object_player_id AS object_indetifier,
		'player'::vertex_type AS object_type,
		edge_type AS edge_type,
		json_build_object (
			'num_games', num_games,
			'subject_points', subject_points,
			'object_points',object_points
		)
	FROM aggregated
)