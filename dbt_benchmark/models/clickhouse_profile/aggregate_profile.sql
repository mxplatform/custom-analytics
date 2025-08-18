{{ config(
    materialized='table'
) }}

WITH events AS (
    SELECT  
        campaign_name,
        toDate(parseDateTimeBestEffortOrNull(
        multiIf(event != 'message_send', arrayElement(event_attributes, 'sendTime'), toString(date)))) AS date,
        event, profile_alt_id
    FROM {{ source('clickhouse', 'event') }}
    PREWHERE account_id = '986'
    and domain = 'event.campaignactivity'
    and platform = 'msg:na'
    and parseDateTimeBestEffortOrNull(
        multiIf(event != 'message_send', arrayElement(event_attributes, 'sendTime'), toString(date))
    ) BETWEEN '2024-01-24 00:00:00' AND '2024-01-26 23:59:59'
    and profile_alt_id is not null
    and event in ('message_send','mesage_open','message_click','message_hard_bounce','message_soft_bounce','message_unsubscribe')
    limit 1000000
),

profile_info AS (
    SELECT
        p_email,
        ms_orig_src, origination, testgroup
    FROM {{ source('clickhouse', 'mi_subscribers_all') }}
    where pk_g4_subscribers_id >= 2000000
),

aggregated AS (
    select a.campaign_name , a.date, event, ms.ms_orig_src, origination, testgroup, 
    case when event = 'message_send' then count(1) end sends,
    case when event = 'message_open' then count(1) end opens,
    case when event = 'message_click' then count(1) end clicks,
    case when event = 'message_open' then count(distinct profile_alt_id) end unique_opens,
    case when event = 'message_click' then count(distinct profile_alt_id) end unique_clicks,
    case when event = 'message_hard_bounce' then count(1) end hard_bounces,
    case when event = 'message_soft_bounce' then count(1) end soft_bounces,
    case when event = 'message_soft_bounce' then count(distinct profile_alt_id) end unique_soft_bounces,
    case when event = 'message_unsubscribe' then count(1) end unsubs,
    case when event = 'message_unsubscribe' then count(distinct profile_alt_id) end unique_unsubs
    FROM events a
    join profile_info ms on a.profile_alt_id = ms.p_email
    group by 1,2,3,4,5,6
    SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 32
)

SELECT a.campaign_name , a.date, a.ms_orig_src, origination, testgroup,
sum(sends) as sends,
sum(opens) as opens,
sum(clicks) as clicks,
sum(unique_opens) as unique_opens,
sum(unique_clicks) as unique_clicks,
sum(hard_bounces) as hard_bounces,
sum(soft_bounces) as soft_bounces,
sum(unique_soft_bounces) as unique_soft_bounces,
sum(unsubs) as unsubs,
sum(unique_unsubs) as unique_unsubs
FROM aggregated a
GROUP BY 1,2,3,4,5