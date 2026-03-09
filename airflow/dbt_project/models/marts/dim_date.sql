WITH source_data AS (
    SELECT * FROM {{ ref('int_listen_events') }}
),
get_date_data AS (
    SELECT DISTINCT unix_ts_minute, TO_TIMESTAMP(unix_ts_minute) AS date_time
    FROM source_data
    UNION
    SELECT DISTINCT registration AS unix_ts_minute, TO_TIMESTAMP(registration) AS date_time
    FROM source_data
),
extract_date_parts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['unix_ts_minute']) }} AS date_key,
        unix_ts_minute,
        date_time,
        EXTRACT(QUARTER FROM date_time) AS quarter,
        EXTRACT(YEAR FROM date_time) AS year,
        EXTRACT(MONTH FROM date_time) AS month,
        EXTRACT(DAY FROM date_time) AS day_of_month,
        EXTRACT(DOW FROM date_time) AS day_of_week,
        EXTRACT(HOUR FROM date_time) AS hour,
        EXTRACT(MINUTE FROM date_time) AS minute,
        CASE WHEN EXTRACT(DOW FROM date_time) IN (6,7) THEN True ELSE False END AS weekend_flag
    FROM get_date_data
)
SELECT * FROM extract_date_parts