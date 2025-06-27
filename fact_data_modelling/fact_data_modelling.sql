-- Fact data modelling

-- Checking for duplicates in game_details table
SELECT
	game_id,
	team_id,
	player_id,
	COUNT(*)
FROM game_details
GROUP BY 1,2,3
HAVING COUNT(*) > 1;

-- DDL for update fact game details

DROP TABLE fct_game_details;

CREATE TABLE fct_game_details(
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
    dim_player_name TEXT,
	dim_start_position TEXT,
	dim_is_playing_at_home BOOLEAN,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_minutes REAL,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_reb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnorvers INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER,
	PRIMARY KEY(dim_game_date, dim_team_id, dim_player_id)
);

-- Deduping the game_details and inserting into fct_game_details

INSERT INTO fct_game_details
	WITH deduped AS(
		SELECT
			ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY g.game_date_est) AS row_num,
			g.game_date_est,
			g.home_team_id,
			g.season,
			gd.*
		FROM game_details gd
		JOIN games g
			ON gd.game_id = g.game_id
	)
	SELECT
		game_date_est AS dim_game_date,
		season AS dim_season,
		team_id AS dim_team_id,
		player_id AS dim_player_id,
	    player_name AS dim_player_name,
		start_position AS dim_start_position,
		team_id = home_team_id AS dim_is_playing_at_home,
		COALESCE(POSITION('DNP' in comment) > 0, false) AS dim_did_not_play,
		COALESCE(POSITION('DND' in comment) > 0, false) AS dim_did_not_dress,
		COALESCE(POSITION('NWT' in comment) > 0, false) AS dim_not_with_team,
		CAST((SPLIT_PART(min,':',1)) AS REAL)
			+ CAST((SPLIT_PART(min,':',2)) AS REAL) AS m_minutes,
		fgm AS m_fgm,
		fga AS m_fga,
		fg3m AS m_fg3m,
		fg3a AS m_fg3a,
		ftm AS m_ftm,
		fta AS m_fta,
		oreb AS m_oreb,
		dreb AS m_dreb,
		reb AS m_reb,
		ast AS m_ast,
		stl AS m_stl,
		blk AS m_blk,
		"TO" as m_turnorvers,
		pf AS m_pf,
		pts AS m_pts,
		plus_minus AS m_plus_minus
	FROM deduped 
	WHERE row_num = 1;

SELECT * FROM fct_game_details;

-- Checking the data
SELECT dim_player_name,
       COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num
FROM fct_game_details
GROUP BY 1
ORDER BY 2 DESC;
