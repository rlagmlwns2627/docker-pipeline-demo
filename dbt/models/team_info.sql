{{ config(materialized='table') }}

SELECT DISTINCT
    home_team_id AS team_id,
    home_team_name AS team_name,
    home_city AS city,
    home_stadium AS stadium,
    home_founded AS founded
FROM {{ source('docker_test_db', 'game_result') }}
WHERE home_team_name IS NOT NULL