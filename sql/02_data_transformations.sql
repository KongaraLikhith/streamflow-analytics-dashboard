-- ============================================
-- STREAMFLOW ANALYTICS: DATA TRANSFORMATIONS
-- ============================================
-- This script cleans and enriches all raw data tables

-- ============================================
-- PART 1: USER TRANSFORMATIONS
-- ============================================

-- 1. Clean Users Table - Add user tenure and segment
CREATE OR REPLACE TABLE `streamflow_analytics.dim_users_clean` AS
SELECT 
    *,
    DATE_DIFF(CURRENT_DATE(), signup_date, DAY) AS days_since_signup,
    CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), signup_date, DAY) <= 30 THEN 'New'
        WHEN DATE_DIFF(CURRENT_DATE(), signup_date, DAY) <= 90 THEN 'Active'
        WHEN DATE_DIFF(CURRENT_DATE(), signup_date, DAY) <= 365 THEN 'Established'
        ELSE 'Veteran'
    END AS user_segment,
    CASE 
        WHEN current_plan = 'Premium' THEN 'High Value'
        WHEN current_plan = 'Family' THEN 'High Value'
        WHEN current_plan = 'Basic' THEN 'Medium Value'
        ELSE 'Low Value'
    END AS value_segment
FROM `streamflow_analytics.dim_users`;

-- ============================================
-- PART 2: CONTENT TRANSFORMATIONS
-- ============================================

-- 2. Clean Content Table - Add content age and performance indicators
CREATE OR REPLACE TABLE `streamflow_analytics.dim_content_clean` AS
SELECT 
    *,
    DATE_DIFF(CURRENT_DATE(), publish_date, DAY) AS days_since_published,
    CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), publish_date, DAY) <= 7 THEN 'New Release'
        WHEN DATE_DIFF(CURRENT_DATE(), publish_date, DAY) <= 30 THEN 'Recent'
        WHEN DATE_DIFF(CURRENT_DATE(), publish_date, DAY) <= 90 THEN 'Catalog'
        ELSE 'Library'
    END AS content_age_segment,
    CASE 
        WHEN duration_minutes < 3 THEN 'Short'
        WHEN duration_minutes < 20 THEN 'Medium'
        WHEN duration_minutes < 60 THEN 'Long'
        ELSE 'Extended'
    END AS duration_segment
FROM `streamflow_analytics.dim_content`;

-- ============================================
-- PART 3: WATCH SESSIONS TRANSFORMATIONS
-- ============================================

-- 3. Enrich Watch Sessions with date parts
CREATE OR REPLACE TABLE `streamflow_analytics.fact_watch_sessions_clean` AS
SELECT 
    *,
    EXTRACT(YEAR FROM watch_start_time) AS watch_year,
    EXTRACT(MONTH FROM watch_start_time) AS watch_month,
    EXTRACT(DAYOFWEEK FROM watch_start_time) AS watch_dayofweek,
    EXTRACT(HOUR FROM watch_start_time) AS watch_hour,
    CASE 
        WHEN EXTRACT(HOUR FROM watch_start_time) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM watch_start_time) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM watch_start_time) BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,
    CASE 
        WHEN completion_rate >= 90 THEN 'Binge Watcher'
        WHEN completion_rate >= 70 THEN 'Engaged'
        WHEN completion_rate >= 40 THEN 'Casual'
        ELSE 'Skimmer'
    END AS engagement_level
FROM `streamflow_analytics.fact_watch_sessions`;

-- 4. Create monthly aggregated fact table for faster queries
CREATE OR REPLACE TABLE `streamflow_analytics.fact_monthly_aggregates` AS
SELECT 
    DATE_TRUNC(watch_start_time, MONTH) AS month,
    user_id,
    COUNT(DISTINCT session_id) AS sessions_per_month,
    SUM(watch_duration_minutes) AS total_watch_minutes,
    AVG(completion_rate) AS avg_completion_rate,
    COUNT(DISTINCT content_id) AS unique_content_watched
FROM `streamflow_analytics.fact_watch_sessions`
GROUP BY 1, 2;

-- ============================================
-- PART 4: SUBSCRIPTION TRANSFORMATIONS
-- ============================================

-- 5. Clean and Enrich Subscriptions Table
CREATE OR REPLACE TABLE `streamflow_analytics.fact_subscriptions_clean` AS
SELECT 
    *,
    EXTRACT(YEAR FROM billing_date) AS billing_year,
    EXTRACT(MONTH FROM billing_date) AS billing_month,
    EXTRACT(QUARTER FROM billing_date) AS billing_quarter,
    DATE_TRUNC(billing_date, MONTH) AS billing_month_start,
    CASE 
        WHEN payment_status = 'Paid' THEN monthly_price 
        ELSE 0 
    END AS recognized_revenue,
    CASE 
        WHEN payment_status = 'Failed' THEN monthly_price 
        ELSE 0 
    END AS failed_revenue,
    CASE 
        WHEN payment_method IN ('Credit Card', 'Debit Card') THEN 'Card'
        WHEN payment_method IN ('PayPal', 'Apple Pay') THEN 'Digital Wallet'
        ELSE 'Other'
    END AS payment_category,
    CASE 
        WHEN plan = 'Premium' THEN 'Tier 3'
        WHEN plan = 'Family' THEN 'Tier 3'
        WHEN plan = 'Basic' THEN 'Tier 2'
        ELSE 'Tier 1'
    END AS plan_tier
FROM `streamflow_analytics.fact_subscriptions`;

-- 6. Create Subscription Metrics Summary Table
CREATE OR REPLACE TABLE `streamflow_analytics.fact_subscription_metrics` AS
WITH user_subscription_history AS (
    SELECT 
        user_id,
        plan,
        billing_date,
        payment_status,
        monthly_price,
        LAG(plan) OVER (PARTITION BY user_id ORDER BY billing_date) AS prev_plan,
        LAG(monthly_price) OVER (PARTITION BY user_id ORDER BY billing_date) AS prev_price,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY billing_date) AS subscription_month_number
    FROM `streamflow_analytics.fact_subscriptions_clean`
    WHERE payment_status = 'Paid'
)
SELECT 
    user_id,
    billing_date,
    plan,
    prev_plan,
    CASE 
        WHEN prev_plan IS NULL THEN 'New Subscription'
        WHEN plan != prev_plan THEN 
            CASE 
                WHEN monthly_price > prev_price THEN 'Upgrade'
                ELSE 'Downgrade'
            END
        ELSE 'Renewal'
    END AS subscription_event_type,
    monthly_price,
    monthly_price - prev_price AS price_change
FROM user_subscription_history;

-- 7. Create Monthly Revenue by Payment Method
CREATE OR REPLACE TABLE `streamflow_analytics.agg_revenue_by_payment` AS
SELECT 
    DATE_TRUNC(billing_date, MONTH) AS month,
    payment_method,
    payment_category,
    plan,
    COUNT(DISTINCT user_id) AS subscribers,
    COUNT(*) AS total_transactions,
    ROUND(SUM(monthly_price), 2) AS gross_revenue,
    ROUND(SUM(recognized_revenue), 2) AS net_revenue,
    ROUND(SUM(failed_revenue), 2) AS failed_amount,
    ROUND(SAFE_DIVIDE(SUM(recognized_revenue), SUM(monthly_price)) * 100, 2) AS success_rate
FROM `streamflow_analytics.fact_subscriptions_clean`
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 6 DESC;

-- 8. Create Customer Lifetime Value by User
CREATE OR REPLACE TABLE `streamflow_analytics.agg_user_ltv` AS
SELECT 
    user_id,
    MIN(billing_date) AS first_payment_date,
    MAX(billing_date) AS last_payment_date,
    DATE_DIFF(MAX(billing_date), MIN(billing_date), MONTH) AS subscription_tenure_months,
    COUNT(DISTINCT billing_month_start) AS paid_months,
    ROUND(SUM(recognized_revenue), 2) AS total_revenue,
    ROUND(AVG(CASE WHEN recognized_revenue > 0 THEN recognized_revenue END), 2) AS avg_monthly_revenue,
    COUNT(CASE WHEN payment_status = 'Failed' THEN 1 END) AS failed_payments,
    ROUND(SAFE_DIVIDE(COUNT(CASE WHEN payment_status = 'Failed' THEN 1 END), COUNT(*)) * 100, 2) AS failure_rate
FROM `streamflow_analytics.fact_subscriptions_clean`
GROUP BY user_id;

-- 9. Create Plan Migration Analysis
CREATE OR REPLACE TABLE `streamflow_analytics.agg_plan_migrations` AS
SELECT 
    prev_plan,
    plan AS new_plan,
    subscription_event_type,
    COUNT(DISTINCT user_id) AS users_migrated,
    COUNT(*) AS total_migrations
FROM `streamflow_analytics.fact_subscription_metrics`
WHERE subscription_event_type IN ('Upgrade', 'Downgrade')
GROUP BY 1, 2, 3
ORDER BY 4 DESC;

-- 10. Monthly Recurring Revenue (MRR) Analysis 
CREATE OR REPLACE TABLE `streamflow_analytics.agg_mrr` AS
WITH monthly_active_subscriptions AS (
    SELECT 
        billing_month_start,
        user_id,
        plan,
        monthly_price,
        billing_date,
        -- Flag for new subscribers (first payment ever)
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY billing_date) AS payment_sequence,
        -- Get next billing month for churn detection
        LEAD(billing_month_start) OVER (
            PARTITION BY user_id 
            ORDER BY billing_month_start
        ) AS next_billing_month
    FROM `streamflow_analytics.fact_subscriptions_clean`
    WHERE payment_status = 'Paid'
),
monthly_metrics AS (
    SELECT 
        billing_month_start,
        plan,
        user_id,
        monthly_price,
        -- New subscriber flag
        CASE WHEN payment_sequence = 1 THEN 1 ELSE 0 END AS is_new,
        -- Churned flag
        CASE 
            WHEN next_billing_month IS NULL THEN 1
            WHEN DATE_DIFF(next_billing_month, billing_month_start, MONTH) > 1 THEN 1
            ELSE 0
        END AS is_churned
    FROM monthly_active_subscriptions
)
SELECT 
    billing_month_start,
    plan,
    COUNT(DISTINCT user_id) AS active_subscribers,
    ROUND(SUM(monthly_price), 2) AS mrr,
    SUM(is_new) AS new_subscribers,
    SUM(is_churned) AS churned_subscribers,
    SUM(is_new) - SUM(is_churned) AS net_subscriber_change
FROM monthly_metrics
GROUP BY billing_month_start, plan
ORDER BY billing_month_start DESC, plan;

-- ============================================
-- VERIFICATION
-- ============================================
SELECT 
    'All Transformations Complete!' as status,
    (SELECT COUNT(*) FROM `streamflow_analytics.dim_users_clean`) as users_clean,
    (SELECT COUNT(*) FROM `streamflow_analytics.dim_content_clean`) as content_clean,
    (SELECT COUNT(*) FROM `streamflow_analytics.fact_watch_sessions_clean`) as sessions_clean,
    (SELECT COUNT(*) FROM `streamflow_analytics.fact_subscriptions_clean`) as subs_clean,
    (SELECT COUNT(*) FROM `streamflow_analytics.agg_mrr`) as mrr_records,
    (SELECT COUNT(*) FROM `streamflow_analytics.agg_user_ltv`) as user_ltv_records;