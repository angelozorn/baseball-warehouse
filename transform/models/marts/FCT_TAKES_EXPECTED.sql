{{ config(
    materialized = 'table',
    partition_by = {'field': 'GAME_DATE', 'data_type': 'date'},
    cluster_by = ['CATCHER_ID']
) }}

WITH TAKES AS (

    SELECT
        *,
        CAST(ROUND(PLATE_X / {{ var('expected_bin_size') }}) AS INT64) AS X_BIN_IDX,
        CAST(ROUND(
            (PLATE_Z_NORM * ({{ var('zone_top') }} - {{ var('zone_bot') }})
                + {{ var('zone_bot') }})
            / {{ var('expected_bin_size') }}
        ) AS INT64) AS Z_BIN_IDX
    FROM {{ ref('FCT_TAKES') }}

)

SELECT
    T.*,
    B.PLATE_X_BIN,
    B.PLATE_Z_BIN,
    B.EXPECTED_CS,
    CAST(T.IS_CALLED_STRIKE AS INT64) - B.EXPECTED_CS AS CS_ABOVE_EXPECTED
FROM TAKES T
INNER JOIN {{ ref('INT_EXPECTED_STRIKE_BY_BIN') }} B
    ON T.X_BIN_IDX = B.X_BIN_IDX
   AND T.Z_BIN_IDX = B.Z_BIN_IDX