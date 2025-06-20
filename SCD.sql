SELECT 
	player_name,
	current_scoring_class,
	is_active
FROM players;

-- Analyse the scoring performance throught out the career seasons respective to the player
CREATE TABLE players _scd(
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)
); 

-- Creating indicators and tracking the scoring class and seasons when player are active
WITH previous_performance AS (
	SELECT
		player_name,
		current_season,
		current_scoring_class,
		LAG(current_scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
		is_active,
		LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
	FROM players
)
SELECT 
	*,
	CASE
		WHEN current_scoring_class <> previous_scoring_class THEN 1
		ELSE 0
	END AS scoring_class_change_indicator,
	CASE
		WHEN is_active <> previous_is_active THEN 1
		ELSE 0
	END AS is_active_change_indicator
FROM previous_performance;
