{{ config(materialized='ephemeral') }}
WITH 
  city_cnt
    AS 
      (
        SELECT 
            user_id
          , session_city
          , COUNT(DISTINCT session_id) AS city_session_count
        FROM {{ ref('src_sessions_eph') }}
        GROUP BY user_id, session_city
      )
SELECT 
    user_id
  , MAX(session_city) AS most_used_city
  , MAX(city_session_count) AS most_used_city_count
FROM city_cnt cc1 
WHERE city_session_count = (SELECT MAX(city_session_count) FROM city_cnt cc2 WHERE cc2.user_id = cc1.user_id AND cc2.session_city = cc1.session_city)
GROUP BY 1 