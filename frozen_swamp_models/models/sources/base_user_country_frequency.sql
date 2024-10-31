{{ config(materialized='ephemeral') }}
WITH
  country_cnt
    AS 
      (
        SELECT 
            user_id 
          , session_country
          , COUNT(DISTINCT session_id) AS country_session_count
        FROM {{ ref('src_sessions_eph') }}
        GROUP BY user_id, session_country
      )
SELECT 
    user_id
  , MAX(session_country) AS most_used_country
  , MAX(country_session_count) AS most_used_country_count
FROM country_cnt cc1
WHERE country_session_count = (SELECT MAX(country_session_count) FROM country_cnt cc2 WHERE cc2.user_id = cc1.user_id AND cc2.session_country = cc1.session_country)
GROUP BY 1 