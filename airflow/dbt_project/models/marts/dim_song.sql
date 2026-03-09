WITH source_data AS (
    SELECT * FROM {{ ref('int_listen_events') }}
),
song_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['song_id']) }} AS song_key,
        song_id,
        song_title,
        key,
        key_confidence,
        loudness,
        mode,
        mode_confidence,
        release_album,
        song_hotness,
        tempo,
        release_year
    FROM source_data
),
dedup_song_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY song_id) AS row_num
    FROM song_data
),
after_dedup_song_data AS (
    SELECT
        song_key,
        song_id,
        song_title,
        key,
        key_confidence,
        loudness,
        mode,
        mode_confidence,
        release_album,
        song_hotness,
        tempo,
        release_year
    FROM dedup_song_data
    WHERE row_num = 1
)
SELECT * FROM after_dedup_song_data