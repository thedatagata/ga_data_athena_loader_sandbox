{{ config(materialized='ephemeral') }}

WITH 
  channel_cnt
    AS 
      (
        SELECT 
            user_id
          , session_marketing_channel 
          , COUNT(DISTINCT session_id) AS channel_session_count
        FROM {{ ref('src_sessions_eph') }}
        GROUP BY user_id, session_marketing_channel
      )
SELECT
    user_id
  , MAX(session_marketing_channel) AS most_used_marketing_channel
  , MAX(channel_session_count) AS most_used_marketing_channel_count
FROM channel_cnt cc1
WHERE channel_session_count = (SELECT MAX(channel_session_count) FROM channel_cnt cc2 WHERE cc2.user_id = cc1.user_id AND cc2.session_marketing_channel = cc1.session_marketing_channel)
GROUP BY 1