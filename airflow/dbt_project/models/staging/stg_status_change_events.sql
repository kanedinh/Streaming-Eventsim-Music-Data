WITH source_data AS (
    SELECT DISTINCT * FROM {{ source('eventsim_data', 'status_change_events') }}
),
q_data AS (
    SELECT
        CAST(EXTRACT(EPOCH FROM DATE_TRUNC('minute', ts)) AS BIGINT) AS unix_ts_minute, -- Convert timestamp to Unix time in seconds, truncated to the minute
        "sessionId" AS session_id,
        auth,
        level AS account_level,
        "itemInSession" AS item_in_session,
        city,
        CAST(zip AS INT) AS zip,
        state AS state_code,
        TRIM(BOTH '"' FROM "userAgent") AS user_agent, -- Remove leading and trailing quotes from userAgent
        lon AS user_longitude,
        lat AS user_latitude,
        "userId" AS user_id,
        "lastName" AS last_name,
        "firstName" AS first_name,
        gender,
        CAST(EXTRACT(EPOCH FROM DATE_TRUNC('minute', TO_TIMESTAMP(registration / 1000.0))) AS BIGINT) AS registration
    FROM source_data
)
SELECT * FROM q_data