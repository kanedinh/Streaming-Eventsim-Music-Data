WITH source_data AS (
    SELECT * FROM {{ ref('int_listen_events') }}
),
artist_data AS (
    SELECT DISTINCT
        artist_id,
        artist_name
    FROM source_data
),
final_artist_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['artist_id', 'artist_name']) }} AS artist_key,
        artist_id,
        artist_name
    FROM artist_data
)
SELECT * FROM final_artist_data