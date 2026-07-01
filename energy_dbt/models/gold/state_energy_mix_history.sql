SELECT
    state_code,
    fuel_type_code,
    fuel_type_name,
    value,
    unit,
    period,
    valid_from,
    valid_to,
    is_current
FROM {{ source('scd', 'state_energy_mix_scd') }}