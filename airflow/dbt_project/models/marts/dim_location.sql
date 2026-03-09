WITH source_data AS (
    SELECT * FROM {{ ref('int_listen_events') }}
),
location_data AS (
    SELECT DISTINCT
        state_code,
        state_name,
        city,
        user_longitude AS longitude,
        user_latitude AS latitude
    FROM source_data
),
final_location_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['state_code', 'city', 'longitude', 'latitude']) }} AS location_key,
        state_code,
        state_name,
        city,
        longitude,
        latitude
    FROM location_data
)
SELECT * FROM final_location_data