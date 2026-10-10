-- The join must neither drop nor duplicate takes.

SELECT A.N AS TAKES, B.N AS TAKES_WITH_EXPECTED
FROM (SELECT COUNT(*) AS N FROM {{ ref('FCT_TAKES') }}) A
CROSS JOIN (SELECT COUNT(*) AS N FROM {{ ref('FCT_TAKES_EXPECTED') }}) B
WHERE A.N != B.N