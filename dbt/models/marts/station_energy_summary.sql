SELECT
    station_id,

    COUNT(*) AS reading_count,

    SUM(energy_kwh) AS total_energy_kwh,

    AVG(energy_kwh) AS avg_energy_kwh,

    MAX(energy_kwh) AS max_energy_kwh,

    MIN(energy_kwh) AS min_energy_kwh,

    AVG(energy_variation_kwh) AS avg_energy_variation_kwh,

    AVG(moving_avg_energy_kwh) AS avg_moving_energy_kwh

FROM {{ ref('stg_station_energy_analysis') }}

GROUP BY station_id

ORDER BY station_id