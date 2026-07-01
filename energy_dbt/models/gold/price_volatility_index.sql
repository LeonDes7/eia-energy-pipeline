WITH gas_prices AS (
    SELECT
        state_code,
        state_name,
        period,
        value AS price_per_mcf
    FROM {{ ref('stg_energy_silver') }}
    WHERE source = 'eia_natural_gas'
      AND metric = 'price_per_mcf'
),

rolling_stats AS (
    SELECT
        state_code,
        state_name,
        period,
        price_per_mcf,
        AVG(price_per_mcf) OVER (PARTITION BY state_code ORDER BY period ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_avg_price,
        STDDEV(price_per_mcf) OVER (PARTITION BY state_code ORDER BY period ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_stddev_price
    FROM gas_prices
)

SELECT
    state_code,
    state_name,
    period,
    price_per_mcf,
    ROUND(rolling_avg_price, 2) AS rolling_avg_price,
    ROUND(rolling_stddev_price, 2) AS rolling_stddev_price,
    ROUND(rolling_stddev_price / NULLIF(rolling_avg_price, 0) * 100, 2) AS volatility_pct
FROM rolling_stats
ORDER BY period DESC, volatility_pct DESC