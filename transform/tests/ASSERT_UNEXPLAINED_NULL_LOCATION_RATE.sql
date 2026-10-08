{{ config(
    severity = 'warn',
    warn_if   = '>0',
    error_if  = '>750'
) }}

SELECT
    GAME_PK,
    AT_BAT_NUMBER,
    PITCH_NUMBER,
    GAME_DATE,
    DESCRIPTION
FROM {{ ref('STG_STATCAST_PITCHES') }}
WHERE (PLATE_X IS NULL OR PLATE_Z IS NULL OR SZ_TOP IS NULL OR SZ_BOT IS NULL)
  AND DESCRIPTION NOT IN ('automatic_ball', 'automatic_strike')
