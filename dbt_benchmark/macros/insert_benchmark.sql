{% macro insert_benchmark() %}
{{ log("Running insert_benchmark macro", info=True) }}
INSERT INTO ds_internal.benchmark (
    platform, region, country, vertical, account_id, account_name, send_date, campaign_type,
    message_type, sto_type, channel_type, sends, bounces, unique_soft_bounces, unique_hard_bounces,
    unique_opens, unique_real_opens, unique_precached_opens, total_clicks, unique_clicks,
    unique_linkclicks, unique_HTMLclicks, unique_textclicks, unique_human_clicks,
    unique_bot_clicks, revenue, order_count, grow_conversion_count
)
SELECT
    cm.brand as platform,
    cm.geo as region,
    cm.Country,
    cm.industry as vertical,
    v.account_id as account_id,
    cm.account_name as account_name,
    toDate(toStartOfMonth(send_date)),
    v.campaign_type,
    null as message_type,
    'Regular' as sto_type,
    v.channel as channel_type,
    sumIf(count, (event = 'message_send')) AS sends, 
    sumIf(count, ((event = 'message_soft_bounce') OR (event = 'message_hard_bounce'))) AS bounces,
    sumIf(count, (event = 'message_soft_bounce')) AS unique_soft_bounces,
    sumIf(count, (event = 'message_hard_bounce')) AS unique_hard_bounces,
    uniqMergeIf(member_state, (event = 'message_open') AND (channel = 'email')) AS unique_opens,
    uniqMergeIf(member_state, (event = 'message_open') AND (channel = 'email') AND NOT is_machine) AS unique_real_opens,
    uniqMergeIf(member_state, (event = 'message_open') AND (channel = 'email') AND NOT is_machine) AS unique_precached_opens,
    sumIf(count, (event = 'message_click')) AS total_clicks,
    uniqMergeIf(member_state, (event = 'message_click') AND (channel = 'email')) AS unique_clicks,
    0 as unique_linkclicks,
    0 as unique_HTMLclicks,
    0 as unique_textclicks, 
    uniqMergeIf(member_state, (event = 'message_click') AND (channel = 'email') AND (NOT is_machine)) AS unique_human_clicks,
    uniqMergeIf(member_state, (event = 'message_click') AND (channel = 'email') AND (is_machine)) AS unique_bot_clicks,
    0 AS revenue,  
    0 AS order_count,  
    0 AS grow_conversion_count
FROM anx_stg1_mercury.msg_totals_bysenddate v
LEFT JOIN ds_internal.customer_metadata cm  ON v.account_id = cm.pls_org_id
WHERE send_date >= '2024-01-24' and send_date < '2024-02-25'
GROUP BY
    cm.brand,
    cm.geo,
    cm.Country,
    cm.industry,
    v.account_id,
    cm.account_name,
    toDate(toStartOfMonth(send_date)),
    v.campaign_type,
    v.channel 
LIMIT 100
{% endmacro %}