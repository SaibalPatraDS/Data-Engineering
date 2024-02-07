-- SHOW CREATE TABLE bootcamp.nba_game_details

/* 
CREATE SCHEMA saibalpatra01
*/


CREATE TABLE saibalpatra01.nba_game_details_sorted (
      player_name VARCHAR,
      player_id BIGINT,
      team_abbreviation VARCHAR,
      points DOUBLE,
      rebounds DOUBLE,
      steals DOUBLE,
      assists DOUBLE,
      game_id BIGINT,
      season INTEGER,
      game_date DATE
)  WITH (
      format = 'PARQUET',
      partitioning = ARRAY['season'],
      sorted_by = ARRAY['player_name', 'game_date']
)


SELECT DISTINCT player_name FROM saibalpatra01.nba_game_details_sorted
-- WHERE player_name LIKE 'A%'

/*
Performing Same in Apache IceBerg
*/

SELECT 'new', SUM(file_size_in_bytes) AS size FROM saibalpatra01."nba_game_details_sorted$files"
UNION ALL
SELECT 'old', SUM(file_size_in_bytes) AS size FROM bootcamp."nba_game_details$files"


INSERT INTO saibalpatra01.nba_game_details_sorted
SELECT gd.player_name,
       gd.player_id,
       gd.team_abbreviation,
       gd.pts AS points,
       gd.reb AS rebounds,
       gd.stl AS steals,
       gd.ast AS assists,
       gd.game_id,
       g.season,
       DATE(g.game_date_est) AS game_date
FROM bootcamp.nba_game_details gd
JOIN bootcamp.nba_games g
ON gd.game_id = g.game_id





/* UNSORTED DATA */

CREATE TABLE saibalpatra01.nba_game_details_unsorted (
      player_name VARCHAR,
      player_id BIGINT,
      team_abbreviation VARCHAR,
      points DOUBLE,
      rebounds DOUBLE,
      steals DOUBLE,
      assists DOUBLE,
      game_id BIGINT,
      season INTEGER,
      game_date DATE
)  WITH (
      format = 'PARQUET',
      partitioning = ARRAY['season']
)

-- INSERTION of DATA

INSERT INTO saibalpatra01.nba_game_details_unsorted
SELECT gd.player_name,
       gd.player_id,
       gd.team_abbreviation,
       gd.pts AS points,
       gd.reb AS rebounds,
       gd.stl AS steals,
       gd.ast AS assists,
       gd.game_id,
       g.season,
       DATE(g.game_date_est) AS game_date
FROM bootcamp.nba_game_details gd
JOIN bootcamp.nba_games g
ON gd.game_id = g.game_id



/* SAVING SPACE */

SELECT 'sorted', SUM(file_size_in_bytes) AS size FROM saibalpatra01."nba_game_details_sorted$files"
UNION ALL
SELECT 'unsorted', SUM(file_size_in_bytes) AS size FROM saibalpatra01."nba_game_details_unsorted$files"


/* SORTING BY Points */

CREATE TABLE saibalpatra01.nba_game_details_sorted_by_points (
      player_name VARCHAR,
      player_id BIGINT,
      team_abbreviation VARCHAR,
      points DOUBLE,
      rebounds DOUBLE,
      steals DOUBLE,
      assists DOUBLE,
      game_id BIGINT,
      season INTEGER,
      game_date DATE
)  WITH (
      format = 'PARQUET',
      partitioning = ARRAY['season'],
      sorted_by = ARRAY['points']
)

-- INSERT INTO THE TABLE

INSERT INTO saibalpatra01.nba_game_details_sorted_by_points
SELECT gd.player_name,
       gd.player_id,
       gd.team_abbreviation,
       gd.pts AS points,
       gd.reb AS rebounds,
       gd.stl AS steals,
       gd.ast AS assists,
       gd.game_id,
       g.season,
       DATE(g.game_date_est) AS game_date
FROM bootcamp.nba_game_details gd
JOIN bootcamp.nba_games g
ON gd.game_id = g.game_id


-- CHECKING STORAGE USED

SELECT 'sorted_by_player_name',
       SUM(file_size_in_bytes) AS size
FROM saibalpatra01."nba_game_details_sorted$files"
UNION ALL 
SELECT 'Unsorted',
       SUM(file_size_in_bytes) AS size
FROM saibalpatra01."nba_game_details_unsorted$files"
UNION ALL 
SELECT 'sorted_by_points',
       SUM(file_size_in_bytes) AS size
FROM saibalpatra01."nba_game_details_sorted_by_points$files"
UNION ALL 
SELECT 'Regular Data',
       SUM(file_size_in_bytes) AS size
FROM bootcamp."nba_game_details$files"




/* HOW MUCH bytes, the processing eats */

SHOW STATS FOR (
     SELECT * FROM saibalpatra01.nba_game_details_sorted
)


/* EXAMPLE 2 */

-- SELECT * FROM bootcamp.actor_films

CREATE TABLE saibalpatra01.actor_films_sorted_by_film (
       actor VARCHAR,
       actor_id VARCHAR,
       film VARCHAR,
       year INTEGER,
       film_id VARCHAR
) WITH (
       format = 'PARQUET',
       partitioning = ARRAY['year'],
       sorted_by = ARRAY['film', 'actor']
)


INSERT INTO saibalpatra01.actor_films_sorted_by_film
SELECT actor, actor_id, film, year, film_id
FROM bootcamp.actor_films

SELECT * 
FROM saibalpatra01.actor_films_sorted_by_film


-- SORT BY Actors

CREATE TABLE saibalpatra01.actor_films_sorted_by_actors (
       actor VARCHAR,
       actor_id VARCHAR,
       film VARCHAR,
       year INTEGER,
       film_id VARCHAR
) WITH (
       format = 'PARQUET',
       partitioning = ARRAY['year'],
       sorted_by = ARRAY['actor', 'film']
)


INSERT INTO saibalpatra01.actor_films_sorted_by_actors 
SELECT actor, actor_id, film, year, film_id
FROM bootcamp.actor_films

SELECT * 
FROM saibalpatra01.actor_films_sorted_by_actors 


-- CHECKING SIZE wise

SELECT 'sort_by_films',
        SUM(file_size_in_bytes) AS size
FROM saibalpatra01."actor_films_sorted_by_film$files"
UNION ALL
SELECT 'sort_by_actors',
        SUM(file_size_in_bytes) AS size
FROM saibalpatra01."actor_films_sorted_by_actors$files"
