SELECT
    station_id,
    timestamp,
    date,
    energy_kwh,
    previous_energy_kwh,
    energy_variation_kwh,
    moving_avg_energy_kwh,
    pump_status

FROM public.station_energy_analysis