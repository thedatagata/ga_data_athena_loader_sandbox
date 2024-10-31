{{ config(materialized='ephemeral') }}

SELECT
    user_id
  , SUM(CASE WHEN session_start_time >= DATE_ADD('day', -1, CURRENT_DATE) THEN session_revenue ELSE 0 END) AS revenue_24h
  , SUM(CASE WHEN session_start_time >= DATE_ADD('day', -7, CURRENT_DATE) THEN session_revenue ELSE 0 END) AS revenue_7d
  , SUM(CASE WHEN session_start_time >= DATE_ADD('day', -30, CURRENT_DATE) THEN session_revenue ELSE 0 END) AS revenue_30d
  , SUM(session_revenue) AS revenue_all_time
  , SUM(CASE WHEN session_start_time >= DATE_ADD('day', -1, CURRENT_DATE) THEN session_order_cnt ELSE 0 END) AS orders_24h
  , SUM(CASE WHEN session_start_time >= DATE_ADD('day', -7, CURRENT_DATE) THEN session_order_cnt ELSE 0 END) AS orders_7d
  , SUM(CASE WHEN session_start_time >= DATE_ADD('day', -30, CURRENT_DATE) THEN session_order_cnt ELSE 0 END) AS orders_30d
  , SUM(session_order_cnt) AS orders_all_time

FROM {{ ref('src_sessions_eph') }}
GROUP BY user_id 