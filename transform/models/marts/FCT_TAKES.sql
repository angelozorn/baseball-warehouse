{{ config(
    materialized = 'table',
    partition_by = {'field': 'GAME_DATE', 'data_type': 'date'},
    cluster_by = ['CATCHER_ID']
) }}

WITH TAKES AS (

    SELECT *
    FROM {{ ref('STG_STATCAST_PITCHES') }}
    WHERE DESCRIPTION IN ('ball', 'called_strike')
      AND PLATE_X IS NOT NULL
      AND PLATE_Z IS NOT NULL
      AND SZ_TOP  IS NOT NULL
      AND SZ_BOT  IS NOT NULL

)

SELECT
    -- grain
    GAME_PK,
    AT_BAT_NUMBER,
    PITCH_NUMBER,
    GAME_DATE,

    -- participants
    CATCHER_ID,
    PITCHER_ID,
    BATTER_ID,

    -- context
    BALLS,
    STRIKES,
    BATTER_STANDS,
    PITCHER_THROWS,
    PITCH_TYPE,
    RELEASE_SPEED,

    -- location, as tracked
    PLATE_X,
    PLATE_Z,
    SZ_TOP,
    SZ_BOT,

    -- location, normalized to this batter's own zone:
    -- 0.0 = bottom of zone, 1.0 = top, outside [0,1] = outside the zone
    SAFE_DIVIDE(PLATE_Z - SZ_BOT, SZ_TOP - SZ_BOT) AS PLATE_Z_NORM,

    -- target
    DESCRIPTION,
    DESCRIPTION = 'called_strike' AS IS_CALLED_STRIKE

FROM TAKES