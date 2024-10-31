    
{{ config(materialized='ephemeral') }}

SELECT
    user_id,
    session_id,
    session_sequence_number,
    session_start_time,
    session_browser,
    session_os,
    session_is_mobile,
    session_device_category,
    session_country,
    session_region,
    session_city,
    session_marketing_channel,
    session_landing_screen,
    session_revenue,
    session_order_cnt,
    session_duration,
    session_is_first_visit,
    CASE WHEN session_revenue > 0 THEN 1 ELSE 0 END AS is_conversion
FROM {{ source('raw_data_staging', 'sessions') }}