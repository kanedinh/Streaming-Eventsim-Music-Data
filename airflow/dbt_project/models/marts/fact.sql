WITH source_data AS (
    SELECT * FROM {{ ref('int_listen_events') }}
),
fact_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["location_key", "date_key", "artist_key", "song_key", "user_key"])}},
        l.location_key,
        d.date_key,
        a.artist_key,
        so.song_key,
        u.user_key
    FROM source_data AS s
    INNER JOIN {{ ref('dim_location') }} AS l
        ON s.state_code = l.state_code
        AND s.city = l.city
        AND s.user_longitude = l.longitude
        AND s.user_latitude = l.latitude
    INNER JOIN {{ ref('dim_date') }} AS d
        ON s.unix_ts_minute = d.unix_ts_minute
    INNER JOIN {{ ref('dim_artist') }} AS a
        ON s.artist_id = a.artist_id
        AND s.artist_name = a.artist_name
    INNER JOIN {{ ref('dim_song') }} AS so
        ON s.song_id = so.song_id
    INNER JOIN {{ ref('dim_user') }} AS u
        ON s.user_id = u.user_id
)
SELECT * FROM fact_data