-- ============================================
-- STREAMFLOW ANALYTICS: TABLE CREATION
-- ============================================
-- This script defines the data warehouse schema
-- Note: Tables were created via BigQuery UI upload
-- This file serves as documentation and reference

-- 1. Users Dimension Table
CREATE TABLE IF NOT EXISTS `streamflow_analytics.dim_users` (
    user_id INT64 NOT NULL,
    username STRING,
    signup_date DATE,
    country STRING,
    age_group STRING,
    primary_device STRING,
    current_plan STRING,
    acquisition_channel STRING
);

-- 2. Content Dimension Table
CREATE TABLE IF NOT EXISTS `streamflow_analytics.dim_content` (
    content_id STRING NOT NULL,
    title STRING,
    category STRING,
    content_type STRING,
    duration_minutes FLOAT64,
    creator_name STRING,
    publish_date DATE,
    is_exclusive BOOL,
    production_quality STRING
);

-- 3. Watch Sessions Fact Table
CREATE TABLE IF NOT EXISTS `streamflow_analytics.fact_watch_sessions` (
    session_id STRING NOT NULL,
    user_id INT64,
    content_id STRING,
    watch_start_time TIMESTAMP,
    watch_duration_minutes FLOAT64,
    content_duration_minutes FLOAT64,
    completion_rate FLOAT64,
    completed BOOL,
    device_used STRING,
    quality_streamed STRING
);

-- 4. Subscriptions Fact Table
CREATE TABLE IF NOT EXISTS `streamflow_analytics.fact_subscriptions` (
    subscription_id STRING NOT NULL,
    user_id INT64,
    plan STRING,
    monthly_price FLOAT64,
    billing_date DATE,
    payment_status STRING,
    payment_method STRING
);

-- Verify tables were created
SELECT 
    'Tables Created:' as info,
    COUNT(*) as table_count
FROM `streamflow_analytics.INFORMATION_SCHEMA.TABLES`
WHERE table_type = 'BASE TABLE';