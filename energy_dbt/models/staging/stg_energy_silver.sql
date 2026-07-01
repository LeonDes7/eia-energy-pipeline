SELECT
    series_id,
    source,
    period,
    state_code,
    state_name,
    fuel_type_code,
    fuel_type_name,
    metric,
    value,
    unit,
    llm_summary
FROM {{ source('silver', 'energy_silver') }}