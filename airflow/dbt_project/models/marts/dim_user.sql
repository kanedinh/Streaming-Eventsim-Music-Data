WITH source_data AS (
    SELECT * FROM {{ ref('int_listen_events') }}
),
user_data AS (
    SELECT
        user_id,
        last_name,
        first_name,
        gender,
        TO_TIMESTAMP(unix_ts_minute) AS updated_at,
        registration AS registration_date,
        level
    FROM source_data
),
dedup_user_data AS (
    SELECT
        user_id,
        last_name,
        first_name,
        gender,
        updated_at,
        registration_date,
        level,
        ROW_NUMBER() OVER (PARTITION BY user_id, level ORDER BY updated_at ASC) AS row_num
    FROM user_data
),
after_dedup_user_data AS (
    SELECT
        user_id,
        last_name,
        first_name,
        gender,
        updated_at AS valid_from,
        registration_date,
        level
    FROM dedup_user_data
    WHERE row_num = 1
),
scd2_user_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['user_id', 'valid_from']) }} AS user_key,
        user_id,
        last_name,
        first_name,
        gender,
        registration_date,
        level,
        valid_from,
        LEAD(valid_from, 1, '9999-12-31'::timestamp) OVER (PARTITION BY user_id ORDER BY valid_from ASC) AS valid_to
    FROM after_dedup_user_data
),
final_user_data AS (
    SELECT
        *,
        CASE
            WHEN valid_to = '9999-12-31'::timestamp THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM scd2_user_data
)
SELECT * FROM final_user_data