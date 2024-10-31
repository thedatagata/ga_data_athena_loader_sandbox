{{ config(materialized='ephemeral') }}

WITH 
  device_cnt
    AS 
      (
        SELECT 
            user_id
          , session_device_category
          , COUNT(DISTINCT session_id) AS device_category_session_count
        FROM {{ ref('src_sessions_eph') }}
        GROUP BY user_id, session_device_category
      )
SELECT
    user_id
  , MAX(session_device_category) AS most_used_device_category
  , MAX(device_category_session_count) AS most_used_device_category_count
FROM device_cnt dc1
WHERE device_category_session_count = (SELECT MAX(device_category_session_count) FROM device_cnt dc2 WHERE dc2.user_id = dc1.user_id AND dc2.session_device_category = dc1.session_device_category)
GROUP BY 1 