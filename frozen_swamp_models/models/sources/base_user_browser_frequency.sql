{{ config(materialized='ephemeral') }}

WITH
  browser_cnt 
    AS 
      (
        SELECT 
            user_id 
          , session_browser 
          , COUNT(DISTINCT session_id) browser_session_count
        FROM {{ ref('src_sessions_eph') }}
        GROUP BY user_id, session_browser
      )

SELECT
  user_id, 
  MAX(session_browser) AS most_used_browser, 
  MAX(browser_session_count) AS most_used_browser_count
FROM browser_cnt bc1
WHERE browser_session_count = (SELECT MAX(browser_session_count) FROM browser_cnt bc2 WHERE bc2.user_id = bc1.user_id AND bc2.session_browser = bc1.session_browser) 
GROUP BY 1 