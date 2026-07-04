WITH generation AS (
    SELECT
        state_code,
        state_name,
        period,
        fuel_type_code,
        value AS generation_mwh
    FROM {{ ref('stg_energy_silver') }}
    WHERE source = 'eia_electricity'
      AND metric = 'generation_mwh'
),

totals AS (
    SELECT
        state_code,
        state_name,
        period,
        SUM(generation_mwh) AS total_generation_mwh,
        SUM(CASE WHEN fuel_type_code IN ('SUN', 'WND', 'HYC') THEN generation_mwh ELSE 0 END) AS renewable_generation_mwh
    FROM generation
    GROUP BY state_code, state_name, period
)

SELECT
    state_code,
    state_name,
    period,
    total_generation_mwh,
    renewable_generation_mwh,
    COALESCE(ROUND(renewable_generation_mwh / NULLIF(total_generation_mwh, 0) * 100, 2), 0) AS renewable_share_pct
FROM totals
ORDER BY period DESC, renewable_share_pct DESC