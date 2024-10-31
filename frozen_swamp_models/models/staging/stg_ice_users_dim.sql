{{ config(
    materialized='table',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['bucket(user_id, 5)'],
) }} 

WITH
    user_session_medians 
        AS 
            (
                SELECT 
                    bf.user_id
                  , bf.most_used_browser 
                  , bf.most_used_browser_count 
                  , of.most_used_os 
                  , of.most_used_os_count 
                  , df.most_used_device_category 
                  , df.most_used_device_category_count
                  , mcf.most_used_marketing_channel
                  , mcf.most_used_marketing_channel_count
                  , cf.most_used_city
                  , cf.most_used_city_count 
                  , ctf.most_used_country 
                  , ctf.most_used_country_count
                FROM {{ ref('base_user_browser_frequency') }} bf
                LEFT JOIN {{ ref('base_user_os_frequency') }} of
                    ON bf.user_id = of.user_id 
                LEFT JOIN {{ ref('base_user_device_frequency') }} df
                    ON bf.user_id = df.user_id
                LEFT JOIN {{ ref('base_user_marketing_channel_frequency') }} mcf
                    ON bf.user_id = mcf.user_id
                LEFT JOIN {{ ref('base_user_city_frequency') }} cf
                    ON bf.user_id = cf.user_id 
                LEFT JOIN {{ ref('base_user_country_frequency') }} ctf
                    ON bf.user_id = ctf.user_id
            )

SELECT 
    b.user_id
  , b.session_count AS user_total_sessions
  , usm.most_used_browser AS user_most_used_browser
  , usm.most_used_browser_count AS user_most_used_browser_count
  , usm.most_used_os AS user_most_used_os
  , usm.most_used_os_count AS user_most_used_os_count
  , usm.most_used_device_category AS user_most_used_device_category
  , usm.most_used_device_category_count AS user_most_used_device_category_count
  , usm.most_used_marketing_channel AS user_most_used_marketing_channel
  , usm.most_used_marketing_channel_count AS user_most_used_marketing_channel_count
  , usm.most_used_city AS user_most_used_city
  , usm.most_used_city_count AS user_most_used_city_count
  , usm.most_used_country AS user_most_used_country
  , usm.most_used_country_count AS user_most_used_country_count
  , b.min_conversion_sequence AS user_first_conversion_session_number 
  , b.median_hour AS user_median_session_hour
  , b.avg_session_duration AS user_avg_session_duration
  , b.mobile_sessions AS user_mobile_sessions
  , b.non_mobile_sessions AS user_non_mobile_sessions
  , b.first_marketing_channel AS user_first_marketing_channel
  , r.revenue_all_time AS user_total_revenue 
  , r.revenue_24h AS user_24h_revenue 
  , r.revenue_7d AS user_7d_revenue 
  , r.revenue_30d AS user_30d_revenue 
  , r.orders_all_time AS user_total_orders 
  , r.orders_24h AS user_24h_orders
  , r.orders_7d AS user_7d_orders 
  , r.orders_30d AS user_30d_orders


FROM {{ref('base_user_features')}} b 
LEFT JOIN {{ref('base_user_revenue')}} r
    ON b.user_id = r.user_id 
LEFT JOIN user_session_medians usm
    ON b.user_id = usm.user_id