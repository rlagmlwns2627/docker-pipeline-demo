{{ config(materialized='table') }}
-- 이 모델을 테이블로 생성하겠다는 dbt 설정

SELECT
    game_id,
    date,
    home_team_id,
    away_team_id,
    innings,
    attendance
FROM {{ source('docker_test_db', 'game_result') }}
-- source(): dbt에게 원본 테이블 위치를 알려주는 함수