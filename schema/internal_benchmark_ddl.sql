DROP TABLE ds_internal.benchmark_new;

CREATE TABLE ds_internal.benchmark_new
(
    region String,                 -- 'NA', 'JP', 'EMEA'
    country String,                -- Country for more granular segmentation
    vertical String,               -- 'Financial Services', 'Media & Publishing', etc.
    time_period Date,              -- Date aggregated to the month level
    promotional_type String,       -- Yes or No value for 'Transactional', 'Non-Transactional'
    sto_type String,               -- 'STO', 'Regular'
    channel_type String,           -- 'Email', 'SMS', 'Push', 'Grow'
    sends UInt64,                  -- Total number of messages sent
    bounces UInt64,                -- Total number of bounces
    unique_soft_bounces UInt64,    -- Unique soft bounces
    unique_hard_bounces UInt64,    -- Unique hard bounces
    unique_opens UInt64,           -- Total unique opens
    unique_real_opens UInt64,      -- Unique real opens (no NHI)
    unique_precached_opens UInt64, -- Unique pre-cached opens
    total_clicks UInt64,           -- Total clicks
    unique_clicks UInt64,          -- Unique clicks
    unique_linkclicks UInt64,      -- Unique link clicks
    unique_human_clicks UInt64,    -- Unique human clicks
    unique_bot_clicks UInt64,      -- Unique bot clicks
    Unsubscribers UInt64,          -- Unsubs
    revenue Float32,               -- Revenue associated with the campaign
    order_count UInt64,            -- Number of orders related to the campaign
    grow_views UInt64,
    grow_conversion_count UInt64   -- Grow conversion count (e.g., experience_entry)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(time_period)  -- Partition by month for efficient queries
ORDER BY (region, vertical, time_period);  -- Sorting key for query efficiency