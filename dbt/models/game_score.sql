{{ config(materialized='table') }}

SELECT
    game_id,
    home_score,
    away_score,
    home_hits,
    away_hits,
    home_errors,
    away_errors,
    score_diff,
    home_win,
    is_extra_inning,
    win_probability
FROM {{ source('docker_test_db', 'game_result') }}