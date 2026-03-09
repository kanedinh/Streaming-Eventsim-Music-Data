WITH stg_data AS (
    SELECT * FROM {{ ref('stg_listen_events') }}
),
add_state_name AS (
    SELECT
        s.*,
        d."stateName" AS state_name
    FROM stg_data AS s
    LEFT JOIN {{ source('eventsim_data', 'state_codes') }} AS d
        ON s.state_code = d."stateCode"
),
add_info AS (
    SELECT
        asn.*,
        s.artist_id,
        s.key,
        s.key_confidence,
        s.loudness,
        s.mode,
        s.mode_confidence,
        s.release AS release_album,
        COALESCE(NULLIF(song_hotttnesss, -1), 0) AS song_hotness,
        s.song_id,
        s.tempo,
        s.track_id,
        s.year AS release_year
    FROM add_state_name AS asn
    LEFT JOIN {{ source('eventsim_data', 'songs') }} AS s
    ON asn.artist_name = s.artist_name
)
SELECT * FROM add_info