{{ config(materialized='ephemeral') }}
WITH
  os_cnt
    AS 
      (
        SELECT 
            user_id 
          , session_os 
          , COUNT(DISTINCT session_id) AS operation_system_count
        FROM {{ ref('src_sessions_eph') }}
        GROUP BY user_id, session_os
      )
SELECT 
    user_id 
  , MAX(session_os) AS most_used_os
  , MAX(operation_system_count) AS most_used_os_count
FROM os_cnt oscnt1
WHERE operation_system_count = (SELECT MAX(operation_system_count) FROM os_cnt oscnt2 WHERE oscnt1.user_id = oscnt2.user_id AND oscnt1.session_os = oscnt2.session_os)     
GROUP BY 1 


