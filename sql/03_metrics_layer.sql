-- ============================================
-- STREAMFLOW ANALYTICS: METRICS LAYER (VIEWS)
-- ============================================
-- Business logic layer for reporting and dashboards

-- 1. Daily KPIs View
CREATE OR REPLACE VIEW `streamflow_analytics.v_daily_kpis` AS
SELECT 
    DATE(ws.watch_start_time) AS date,
    COUNT(DISTINCT ws.user_id) AS daily_active_users,
    COUNT(DISTINCT ws.session_id) AS total_sessions,
    ROUND(AVG(ws.watch_duration_minutes), 2) AS avg_watch_time_mins,
    ROUND(AVG(ws.completion_rate), 2) AS avg_completion_rate,
    SUM(CASE WHEN ws.completed THEN 1 ELSE 0 END) AS completed_views,
    COUNT(DISTINCT c.content_id) AS unique_content_watched,
    COUNT(DISTINCT c.creator_name) AS unique_creators_watched
FROM `streamflow_analytics.fact_watch_sessions_clean` ws
JOIN `streamflow_analytics.dim_content_clean` c ON ws.content_id = c.content_id
GROUP BY DATE(ws.watch_start_time);

-- 2. Creator Performance View
CREATE OR REPLACE VIEW `streamflow_analytics.v_creator_performance` AS
WITH creator_stats AS (
    SELECT 
        c.creator_name,
        COUNT(DISTINCT c.content_id) AS total_content_pieces,
        COUNT(DISTINCT ws.session_id) AS total_views,
        COUNT(DISTINCT ws.user_id) AS unique_viewers,
        ROUND(AVG(ws.watch_duration_minutes), 2) AS avg_watch_time,
        ROUND(AVG(ws.completion_rate), 2) AS avg_completion_rate,
        ROUND(SUM(ws.watch_duration_minutes) / 60, 2) AS total_watch_hours
    FROM `streamflow_analytics.dim_content_clean` c
    LEFT JOIN `streamflow_analytics.fact_watch_sessions_clean` ws 
        ON c.content_id = ws.content_id
    GROUP BY c.creator_name
)
SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY total_views DESC) AS popularity_rank
FROM creator_stats
WHERE total_views > 0
ORDER BY total_views DESC;

-- 3. Revenue Summary View
CREATE OR REPLACE VIEW `streamflow_analytics.v_revenue_summary` AS
SELECT 
    DATE_TRUNC(billing_date, MONTH) AS revenue_month,
    plan,
    COUNT(DISTINCT user_id) AS subscribers,
    ROUND(SUM(monthly_price), 2) AS gross_revenue,
    ROUND(SUM(CASE WHEN payment_status = 'Paid' THEN monthly_price ELSE 0 END), 2) AS net_revenue,
    ROUND(AVG(CASE WHEN payment_status = 'Paid' THEN monthly_price END), 2) AS arpu,
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN payment_status = 'Paid' THEN monthly_price ELSE 0 END), 
           SUM(monthly_price)) * 100, 2) AS payment_success_rate
FROM `streamflow_analytics.fact_subscriptions`
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- 4. Category Performance View
CREATE OR REPLACE VIEW `streamflow_analytics.v_category_performance` AS
SELECT 
    c.category,
    COUNT(DISTINCT c.content_id) AS content_count,
    COUNT(DISTINCT ws.session_id) AS total_views,
    COUNT(DISTINCT ws.user_id) AS unique_viewers,
    ROUND(AVG(ws.completion_rate), 2) AS avg_completion_rate,
    ROUND(SUM(ws.watch_duration_minutes) / 60, 2) AS total_watch_hours,
    ROUND(AVG(ws.watch_duration_minutes), 2) AS avg_watch_time_mins
FROM `streamflow_analytics.dim_content_clean` c
LEFT JOIN `streamflow_analytics.fact_watch_sessions_clean` ws 
    ON c.content_id = ws.content_id
GROUP BY c.category
ORDER BY total_views DESC;

-- 5. Plan Engagement View
CREATE OR REPLACE VIEW `streamflow_analytics.v_plan_engagement` AS
SELECT 
    u.current_plan,
    u.value_segment,
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT ws.session_id) AS total_sessions,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT ws.session_id), COUNT(DISTINCT u.user_id)), 2) AS sessions_per_user,
    ROUND(AVG(ws.watch_duration_minutes), 2) AS avg_watch_time,
    ROUND(AVG(ws.completion_rate), 2) AS avg_completion_rate
FROM `streamflow_analytics.dim_users_clean` u
LEFT JOIN `streamflow_analytics.fact_watch_sessions_clean` ws 
    ON u.user_id = ws.user_id
GROUP BY 1, 2
ORDER BY total_sessions DESC;

-- 6. Top Content View
CREATE OR REPLACE VIEW `streamflow_analytics.v_top_content` AS
SELECT 
    c.title,
    c.creator_name,
    c.category,
    c.content_type,
    c.duration_segment,
    COUNT(DISTINCT ws.session_id) AS total_views,
    COUNT(DISTINCT ws.user_id) AS unique_viewers,
    ROUND(AVG(ws.completion_rate), 2) AS avg_completion_rate,
    ROUND(AVG(ws.watch_duration_minutes), 2) AS avg_watch_time
FROM `streamflow_analytics.dim_content_clean` c
LEFT JOIN `streamflow_analytics.fact_watch_sessions_clean` ws 
    ON c.content_id = ws.content_id
GROUP BY 1, 2, 3, 4, 5
HAVING total_views > 0
ORDER BY total_views DESC
LIMIT 100;

-- 7. MRR Trends View
CREATE OR REPLACE VIEW `streamflow_analytics.v_mrr_trends` AS
SELECT 
    billing_month_start AS month,
    plan,
    active_subscribers,
    mrr,
    new_subscribers,
    churned_subscribers,
    net_subscriber_change,
    ROUND(SAFE_DIVIDE(mrr, active_subscribers), 2) AS arpu,
    ROUND(SAFE_DIVIDE(churned_subscribers, (active_subscribers - net_subscriber_change)) * 100, 2) AS churn_rate
FROM `streamflow_analytics.agg_mrr`
ORDER BY 1 DESC, 2;

-- 8. Payment Method Performance View
CREATE OR REPLACE VIEW `streamflow_analytics.v_payment_performance` AS
SELECT 
    month,
    payment_method,
    payment_category,
    subscribers,
    total_transactions,
    net_revenue,
    gross_revenue,
    failed_amount,
    success_rate
FROM `streamflow_analytics.agg_revenue_by_payment`
ORDER BY 1 DESC, 6 DESC;

-- 9. User LTV Distribution View
CREATE OR REPLACE VIEW `streamflow_analytics.v_ltv_distribution` AS
SELECT 
    CASE 
        WHEN total_revenue = 0 THEN '$0'
        WHEN total_revenue < 50 THEN '< $50'
        WHEN total_revenue < 100 THEN '$50 - $100'
        WHEN total_revenue < 250 THEN '$100 - $250'
        WHEN total_revenue < 500 THEN '$250 - $500'
        ELSE '$500+'
    END AS ltv_bracket,
    COUNT(*) AS user_count,
    ROUND(AVG(subscription_tenure_months), 1) AS avg_tenure_months,
    ROUND(AVG(avg_monthly_revenue), 2) AS avg_monthly_spend,
    ROUND(AVG(failure_rate), 2) AS avg_failure_rate
FROM `streamflow_analytics.agg_user_ltv`
GROUP BY 1
ORDER BY MIN(total_revenue);

-- 10. Revenue Health Dashboard View
CREATE OR REPLACE VIEW `streamflow_analytics.v_revenue_health` AS
WITH current_month AS (
    SELECT 
        SUM(mrr) AS current_mrr,
        SUM(active_subscribers) AS current_subscribers,
        SUM(new_subscribers) AS new_this_month,
        SUM(churned_subscribers) AS churned_this_month
    FROM `streamflow_analytics.agg_mrr`
    WHERE billing_month_start = DATE_TRUNC(CURRENT_DATE(), MONTH)
),
previous_month AS (
    SELECT 
        SUM(mrr) AS prev_mrr,
        SUM(active_subscribers) AS prev_subscribers
    FROM `streamflow_analytics.agg_mrr`
    WHERE billing_month_start = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
)
SELECT 
    ROUND(cm.current_mrr, 2) AS current_mrr,
    cm.current_subscribers,
    cm.new_this_month,
    cm.churned_this_month,
    ROUND(cm.current_mrr - pm.prev_mrr, 2) AS mrr_change,
    ROUND(SAFE_DIVIDE((cm.current_mrr - pm.prev_mrr), pm.prev_mrr) * 100, 2) AS mrr_growth_pct,
    ROUND(SAFE_DIVIDE(cm.churned_this_month, pm.prev_subscribers) * 100, 2) AS monthly_churn_rate
FROM current_month cm
CROSS JOIN previous_month pm;

-- 11. Executive Summary View (One-row overview)
CREATE OR REPLACE VIEW `streamflow_analytics.v_executive_summary` AS
SELECT 
    (SELECT COUNT(DISTINCT user_id) FROM `streamflow_analytics.dim_users`) AS total_users,
    (SELECT COUNT(DISTINCT content_id) FROM `streamflow_analytics.dim_content`) AS total_content,
    (SELECT COUNT(DISTINCT session_id) FROM `streamflow_analytics.fact_watch_sessions`) AS total_watch_sessions,
    (SELECT ROUND(SUM(watch_duration_minutes) / 60, 0) FROM `streamflow_analytics.fact_watch_sessions`) AS total_watch_hours,
    (SELECT ROUND(AVG(completion_rate), 2) FROM `streamflow_analytics.fact_watch_sessions`) AS avg_completion_rate,
    (SELECT ROUND(SUM(monthly_price), 2) FROM `streamflow_analytics.fact_subscriptions` WHERE payment_status = 'Paid') AS lifetime_revenue,
    (SELECT COUNT(DISTINCT user_id) FROM `streamflow_analytics.fact_subscriptions` WHERE payment_status = 'Paid') AS paying_users;

-- Verification
SELECT 
    'Metrics Layer Complete!' as status,
    (SELECT COUNT(*) FROM `streamflow_analytics.INFORMATION_SCHEMA.VIEWS`) as total_views_created;