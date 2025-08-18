{{ config(
    materialized='table'
) }}

WITH raw_data AS (
SELECT
    cm.geo as region,
    cm.Country,
    cm.industry as vertical,
    toDate(toStartOfMonth(send_date)) as time_period,
    v.campaign_type,
    'Regular' as sto_type,
    v.channel as channel_type,
    sumIf(1, (event = 'message_send')) AS sends, 
    sumIf(1, ((event = 'message_soft_bounce') OR (event = 'message_hard_bounce'))) AS bounces,
    sumIf(1, (event = 'message_soft_bounce')) AS unique_soft_bounces,
    sumIf(1, (event = 'message_hard_bounce')) AS unique_hard_bounces,
    uniqMergeIf(member_state, (event = 'message_open') AND (channel = 'email')) AS unique_opens,
    uniqMergeIf(member_state, (event = 'message_open') AND (channel = 'email')) AS unique_real_opens,
    uniqMergeIf(member_state, (event = 'message_open') AND (channel = 'email')) AS unique_precached_opens,
    sumIf(1, (event = 'message_click')) AS total_clicks,
    uniqMergeIf(member_state, (event = 'message_click') AND (channel = 'email')) AS unique_clicks,
    uniqMergeIf(member_state, (event = 'message_click') AND (channel = 'email') AND (NOT is_machine)) AS unique_human_clicks,
    uniqMergeIf(member_state, (event = 'message_click') AND (channel = 'email') AND (is_machine)) AS unique_bot_clicks,
    0 AS revenue,  
    0 AS order_count,
    0 as grow_views,
    0 AS grow_conversion_count
FROM {{ source('clickhouse', 'msg_totals_bysenddate_v') }} v
LEFT JOIN {{ source('ds_internal', 'customer_metadata') }} cm ON v.account_id = cm.pls_org_id
--ds_internal.customer_metadata cm  ON v.account_id = cm.pls_org_id
WHERE --domain = 'event.campaignactivity'
 --and platform = 'msg:na'
    --and v.account_id = '986'
 --and v.send_date > '2025-01-01' AND '2025-01-31'  -- Filter by a relevant date range
  send_date >= '2022-01-24' and send_date < '2025-05-25'
GROUP BY
    cm.brand,
    cm.geo,
    cm.Country,
    cm.industry,
    toDate(toStartOfMonth(send_date)),
    v.campaign_type,
    v.campaign_type,
    v.channel 
)

SELECT * FROM raw_data
