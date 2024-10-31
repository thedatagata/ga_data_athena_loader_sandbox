{{ config(materialized='ephemeral') }}

SELECT
    user_id
  , COUNT(DISTINCT session_id) AS session_count
  , MIN(CASE WHEN is_conversion = 1 THEN session_sequence_number ELSE NULL END) AS min_conversion_sequence
  , APPROX_PERCENTILE(EXTRACT(hour FROM session_start_time), 0.5) AS median_hour
  , AVG(session_duration) AS avg_session_duration
  , SUM(CASE WHEN session_is_mobile THEN 1 ELSE 0 END) AS mobile_sessions
  , SUM(CASE WHEN NOT session_is_mobile THEN 1 ELSE 0 END) AS non_mobile_sessions
  , MIN(CASE WHEN session_is_first_visit THEN session_marketing_channel ELSE NULL END) AS first_marketing_channel 
FROM {{ref('src_sessions_eph')}} 
GROUP BY user_id