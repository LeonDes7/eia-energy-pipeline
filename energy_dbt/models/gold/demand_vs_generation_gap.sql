WITH generation AS (
    SELECT
        state_code,
        state_name,
        period,
        SUM(value) AS total_generation_mwh
    FROM {{ ref('stg_energy_silver') }}
    WHERE source = 'eia_electricity'
      AND metric = 'generation_mwh'
    GROUP BY state_code, state_name, period
),

renewable_only AS (
    SELECT
        state_code,
        state_name,
        period,
        SUM(value) AS renewable_generation_mwh
    FROM {{ ref('stg_energy_silver') }}
    WHERE source = 'eia_renewable'
      AND metric = 'generation_mwh'
    GROUP BY state_code, state_name, period
)

SELECT
    g.state_code,
    g.state_name,
    g.period,
    g.total_generation_mwh,
    COALESCE(r.renewable_generation_mwh, 0) AS renewable_generation_mwh,
    g.total_generation_mwh - COALESCE(r.renewable_generation_mwh, 0) AS non_renewable_generation_mwh
FROM generation g
LEFT JOIN renewable_only r
    ON g.state_code = r.state_code AND g.period = r.period
ORDER BY g.period DESC, g.total_generation_mwh DESC