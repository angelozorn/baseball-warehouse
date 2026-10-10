-- Every pitch's expectation is its own bin's mean, so the row-weighted
-- average of EXPECTED_CS must equal the actual called-strike rate exactly.
-- This is the invariant a Tableau FIXED LOD violated by weighting bins
-- equally instead of by row count: 0.049 instead of 0.324.

SELECT
    AVG(CAST(IS_CALLED_STRIKE AS INT64)) AS ACTUAL_RATE,
    AVG(EXPECTED_CS)                     AS EXPECTED_RATE
FROM {{ ref('FCT_TAKES_EXPECTED') }}
HAVING ABS(AVG(CAST(IS_CALLED_STRIKE AS INT64)) - AVG(EXPECTED_CS)) > 0.0001