{{ config(materialized = 'table') }}

-- League called-strike rate per location bin. This is the empirical
-- expected-strike probability: what any catcher would be expected to get
-- on a pitch in this location.
--
-- Bins are stored as integer indices rather than float coordinates, because
-- joining on floats is only safe for bin sizes that are exact in binary.
-- 0.25 is; 0.1 is not. Indices make the join correct at any bin size.

WITH BINNED AS (

    SELECT
        CAST(ROUND(PLATE_X / {{ var('expected_bin_size') }}) AS INT64) AS X_BIN_IDX,
        CAST(ROUND(
            (PLATE_Z_NORM * ({{ var('zone_top') }} - {{ var('zone_bot') }})
                + {{ var('zone_bot') }})
            / {{ var('expected_bin_size') }}
        ) AS INT64) AS Z_BIN_IDX,
        IS_CALLED_STRIKE
    FROM {{ ref('FCT_TAKES') }}

)

SELECT
    X_BIN_IDX,
    Z_BIN_IDX,
    X_BIN_IDX * {{ var('expected_bin_size') }} AS PLATE_X_BIN,
    Z_BIN_IDX * {{ var('expected_bin_size') }} AS PLATE_Z_BIN,
    COUNT(*)                                   AS TAKES_IN_BIN,
    AVG(CAST(IS_CALLED_STRIKE AS INT64))       AS EXPECTED_CS
FROM BINNED
GROUP BY 1, 2