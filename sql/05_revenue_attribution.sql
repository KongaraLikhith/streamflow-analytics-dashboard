-- ============================================
-- STREAMFLOW ANALYTICS: REVENUE ATTRIBUTION (FIXED)
-- ============================================
-- Advanced revenue attribution modeling
-- Attributes subscription revenue to content/creators that drove conversions

-- ============================================
-- 1. CONTENT ATTRIBUTION (FIRST-TOUCH MODEL)
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_content_revenue_attribution` AS
WITH user_first_watch AS (
    SELECT 
        ws.user_id,
        ws.content_id,
        c.title,
        c.creator_name,
        c.category,
        ws.watch_start_time,
        ROW_NUMBER() OVER (PARTITION BY ws.user_id ORDER BY ws.watch_start_time) AS watch_order
    FROM `streamflow_analytics.fact_watch_sessions_clean` ws
    JOIN `streamflow_analytics.dim_content_clean` c ON ws.content_id = c.content_id
),
user_subscription_start AS (
    SELECT 
        user_id,
        MIN(billing_date) AS first_payment_date,
        plan AS first_plan
    FROM `streamflow_analytics.fact_subscriptions_clean`
    WHERE payment_status = 'Paid'
    GROUP BY user_id, plan
),
attributed_revenue AS (
    SELECT 
        ufw.user_id,
        ufw.content_id,
        ufw.title,
        ufw.creator_name,
        ufw.category,
        uss.first_plan,
        uss.first_payment_date,
        COALESCE((
            SELECT SUM(recognized_revenue) 
            FROM `streamflow_analytics.fact_subscriptions_clean` s
            WHERE s.user_id = ufw.user_id AND s.payment_status = 'Paid'
        ), 0) AS user_lifetime_revenue
    FROM user_first_watch ufw
    JOIN user_subscription_start uss ON ufw.user_id = uss.user_id
    WHERE ufw.watch_order = 1
      AND DATE(ufw.watch_start_time) <= uss.first_payment_date
)
SELECT 
    creator_name,
    category,
    COUNT(DISTINCT user_id) AS attributed_conversions,
    COUNT(DISTINCT content_id) AS unique_content_driving_conversions,
    ROUND(SUM(user_lifetime_revenue), 2) AS attributed_revenue,
    ROUND(AVG(user_lifetime_revenue), 2) AS avg_revenue_per_converted_user
FROM attributed_revenue
GROUP BY 1, 2
ORDER BY attributed_revenue DESC;

-- ============================================
-- 2. CREATOR ROI ANALYSIS
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_creator_roi` AS
WITH creator_revenue AS (
    SELECT 
        creator_name,
        SUM(attributed_revenue) AS total_attributed_revenue,
        SUM(attributed_conversions) AS total_conversions
    FROM `streamflow_analytics.v_content_revenue_attribution`
    GROUP BY creator_name
),
creator_engagement AS (
    SELECT 
        c.creator_name,
        COUNT(DISTINCT c.content_id) AS total_content,
        COUNT(DISTINCT ws.session_id) AS total_views,
        ROUND(SUM(ws.watch_duration_minutes) / 60, 2) AS total_watch_hours,
        ROUND(AVG(ws.completion_rate), 2) AS avg_completion_rate
    FROM `streamflow_analytics.dim_content_clean` c
    LEFT JOIN `streamflow_analytics.fact_watch_sessions_clean` ws 
        ON c.content_id = ws.content_id
    GROUP BY c.creator_name
)
SELECT 
    ce.creator_name,
    ce.total_content,
    ce.total_views,
    ce.total_watch_hours,
    ce.avg_completion_rate,
    COALESCE(cr.total_conversions, 0) AS attributed_conversions,
    COALESCE(cr.total_attributed_revenue, 0) AS attributed_revenue,
    ROUND(SAFE_DIVIDE(COALESCE(cr.total_attributed_revenue, 0), NULLIF(ce.total_content, 0)), 2) AS revenue_per_content,
    ROUND(SAFE_DIVIDE(COALESCE(cr.total_attributed_revenue, 0), NULLIF(ce.total_views, 0)) * 1000, 2) AS revenue_per_1k_views,
    ROUND(
        (SAFE_DIVIDE(COALESCE(cr.total_conversions, 0), NULLIF(ce.total_views, 0)) * 1000 * 0.4 +
         SAFE_DIVIDE(ce.avg_completion_rate, 100) * 0.3 +
         SAFE_DIVIDE(COALESCE(cr.total_attributed_revenue, 0), NULLIF(ce.total_content, 0)) / 100 * 0.3) * 100,
        2
    ) AS creator_efficiency_score
FROM creator_engagement ce
LEFT JOIN creator_revenue cr ON ce.creator_name = cr.creator_name
WHERE ce.total_views > 0
ORDER BY attributed_revenue DESC;

-- ============================================
-- 3. CATEGORY REVENUE ATTRIBUTION
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_category_revenue_attribution` AS
SELECT 
    category,
    SUM(attributed_conversions) AS total_conversions,
    SUM(attributed_revenue) AS total_attributed_revenue,
    COUNT(DISTINCT creator_name) AS creators_contributing,
    ROUND(AVG(avg_revenue_per_converted_user), 2) AS avg_revenue_per_user
FROM `streamflow_analytics.v_content_revenue_attribution`
GROUP BY category
ORDER BY total_attributed_revenue DESC;

-- ============================================
-- 4. CONVERSION FUNNEL ANALYSIS (FIXED)
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_conversion_funnel` AS
WITH user_journey AS (
    SELECT 
        u.user_id,
        u.signup_date,
        u.current_plan,
        MIN(ws.watch_start_time) AS first_watch_date,
        COUNT(DISTINCT ws.session_id) AS total_sessions_before_conversion,
        AVG(ws.completion_rate) AS avg_completion_before_conversion,
        MIN(s.billing_date) AS subscription_date
    FROM `streamflow_analytics.dim_users_clean` u
    LEFT JOIN `streamflow_analytics.fact_watch_sessions_clean` ws 
        ON u.user_id = ws.user_id
    LEFT JOIN `streamflow_analytics.fact_subscriptions_clean` s 
        ON u.user_id = s.user_id AND s.payment_status = 'Paid'
    WHERE DATE(ws.watch_start_time) <= COALESCE(s.billing_date, CURRENT_DATE())
    GROUP BY 1, 2, 3
)
SELECT 
    current_plan,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN subscription_date IS NOT NULL THEN user_id END) AS converted_users,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN subscription_date IS NOT NULL THEN user_id END), 
          COUNT(DISTINCT user_id)) * 100, 2) AS conversion_rate,
    ROUND(AVG(total_sessions_before_conversion), 1) AS avg_sessions_to_convert,
    ROUND(AVG(avg_completion_before_conversion), 2) AS avg_completion_rate,
    ROUND(AVG(DATE_DIFF(subscription_date, DATE(first_watch_date), DAY)), 1) AS avg_days_to_convert
FROM user_journey
GROUP BY 1
ORDER BY conversion_rate DESC;

-- ============================================
-- 5. TIME-TO-CONVERT ANALYSIS (FIXED)
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_time_to_convert` AS
WITH conversion_timeline AS (
    SELECT 
        u.user_id,
        u.signup_date,
        u.acquisition_channel,
        MIN(ws.watch_start_time) AS first_watch,
        MIN(s.billing_date) AS first_payment,
        DATE_DIFF(MIN(s.billing_date), DATE(MIN(ws.watch_start_time)), DAY) AS days_to_convert
    FROM `streamflow_analytics.dim_users_clean` u
    JOIN `streamflow_analytics.fact_watch_sessions_clean` ws ON u.user_id = ws.user_id
    JOIN `streamflow_analytics.fact_subscriptions_clean` s ON u.user_id = s.user_id
    WHERE s.payment_status = 'Paid'
    GROUP BY 1, 2, 3
)
SELECT 
    CASE 
        WHEN days_to_convert <= 1 THEN 'Same Day'
        WHEN days_to_convert <= 7 THEN 'Within 1 Week'
        WHEN days_to_convert <= 30 THEN 'Within 1 Month'
        WHEN days_to_convert <= 90 THEN 'Within 3 Months'
        ELSE '3+ Months'
    END AS conversion_timeframe,
    acquisition_channel,
    COUNT(DISTINCT user_id) AS users_converted,
    ROUND(AVG(days_to_convert), 1) AS avg_days
FROM conversion_timeline
WHERE days_to_convert >= 0
GROUP BY 1, 2
ORDER BY MIN(days_to_convert), acquisition_channel;

-- ============================================
-- VERIFICATION
-- ============================================
SELECT 
    'Revenue Attribution Complete!' as status,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_content_revenue_attribution`) as attribution_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_creator_roi`) as roi_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_category_revenue_attribution`) as category_attribution_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_conversion_funnel`) as funnel_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_time_to_convert`) as time_to_convert_rows;