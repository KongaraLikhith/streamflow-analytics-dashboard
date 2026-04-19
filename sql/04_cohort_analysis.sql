-- ============================================
-- STREAMFLOW ANALYTICS: COHORT ANALYSIS
-- ============================================
-- Advanced retention, LTV, and churn cohort analysis
-- Contains 8 views for comprehensive cohort analytics

-- ============================================
-- 1. USER COHORT RETENTION (MONTHLY)
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_cohort_retention` AS
WITH user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC(signup_date, MONTH) AS cohort_month
    FROM `streamflow_analytics.dim_users_clean`
),
user_activity AS (
    SELECT 
        DISTINCT
        ws.user_id,
        DATE_TRUNC(DATE(ws.watch_start_time), MONTH) AS activity_month
    FROM `streamflow_analytics.fact_watch_sessions_clean` ws
),
cohort_data AS (
    SELECT 
        uc.cohort_month,
        DATE_DIFF(ua.activity_month, uc.cohort_month, MONTH) AS months_since_signup,
        COUNT(DISTINCT uc.user_id) AS active_users
    FROM user_cohorts uc
    LEFT JOIN user_activity ua ON uc.user_id = ua.user_id
    WHERE ua.activity_month >= uc.cohort_month
    GROUP BY 1, 2
),
cohort_sizes AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM user_cohorts
    GROUP BY 1
)
SELECT 
    cd.cohort_month,
    cd.months_since_signup,
    cd.active_users,
    cs.cohort_size,
    ROUND(SAFE_DIVIDE(cd.active_users, cs.cohort_size) * 100, 2) AS retention_rate
FROM cohort_data cd
JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
WHERE cd.months_since_signup >= 0
  AND cd.months_since_signup <= 12
ORDER BY cd.cohort_month DESC, cd.months_since_signup;

-- ============================================
-- 2. RETENTION MATRIX (PIVOT TABLE FOR HEATMAP)
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_retention_matrix` AS
WITH cohort_retention AS (
    SELECT 
        cohort_month,
        months_since_signup,
        retention_rate
    FROM `streamflow_analytics.v_cohort_retention`
    WHERE months_since_signup <= 12
)
SELECT 
    FORMAT_DATE('%Y-%m', cohort_month) AS cohort,
    ROUND(MAX(CASE WHEN months_since_signup = 0 THEN retention_rate END), 1) AS month_0,
    ROUND(MAX(CASE WHEN months_since_signup = 1 THEN retention_rate END), 1) AS month_1,
    ROUND(MAX(CASE WHEN months_since_signup = 2 THEN retention_rate END), 1) AS month_2,
    ROUND(MAX(CASE WHEN months_since_signup = 3 THEN retention_rate END), 1) AS month_3,
    ROUND(MAX(CASE WHEN months_since_signup = 4 THEN retention_rate END), 1) AS month_4,
    ROUND(MAX(CASE WHEN months_since_signup = 5 THEN retention_rate END), 1) AS month_5,
    ROUND(MAX(CASE WHEN months_since_signup = 6 THEN retention_rate END), 1) AS month_6,
    ROUND(MAX(CASE WHEN months_since_signup = 12 THEN retention_rate END), 1) AS month_12,
    ROUND((
        MAX(CASE WHEN months_since_signup = 1 THEN retention_rate END) +
        MAX(CASE WHEN months_since_signup = 3 THEN retention_rate END) +
        MAX(CASE WHEN months_since_signup = 6 THEN retention_rate END)
    ) / 3, 1) AS avg_retention_6m
FROM cohort_retention
WHERE months_since_signup IN (0, 1, 2, 3, 4, 5, 6, 12)
GROUP BY cohort_month
HAVING MAX(CASE WHEN months_since_signup = 0 THEN retention_rate END) IS NOT NULL
ORDER BY cohort_month DESC
LIMIT 12;

-- ============================================
-- 3. LIFETIME VALUE (LTV) BY COHORT
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_ltv_by_cohort` AS
WITH user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC(signup_date, MONTH) AS cohort_month,
        current_plan
    FROM `streamflow_analytics.dim_users_clean`
),
user_revenue AS (
    SELECT 
        u.user_id,
        u.cohort_month,
        u.current_plan,
        COALESCE(SUM(s.monthly_price), 0) AS lifetime_revenue,
        COUNT(DISTINCT s.billing_date) AS months_subscribed,
        MIN(s.billing_date) AS first_payment_date,
        MAX(s.billing_date) AS last_payment_date
    FROM user_cohorts u
    LEFT JOIN `streamflow_analytics.fact_subscriptions` s 
        ON u.user_id = s.user_id 
        AND s.payment_status = 'Paid'
    GROUP BY 1, 2, 3
)
SELECT 
    cohort_month,
    COUNT(DISTINCT user_id) AS users_in_cohort,
    COUNT(DISTINCT CASE WHEN lifetime_revenue > 0 THEN user_id END) AS paying_users,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN lifetime_revenue > 0 THEN user_id END), 
          COUNT(DISTINCT user_id)) * 100, 2) AS conversion_rate,
    ROUND(AVG(lifetime_revenue), 2) AS avg_ltv,
    ROUND(AVG(CASE WHEN lifetime_revenue > 0 THEN lifetime_revenue END), 2) AS avg_ltv_paying,
    ROUND(AVG(months_subscribed), 1) AS avg_subscription_months,
    ROUND(SUM(lifetime_revenue), 2) AS total_cohort_revenue
FROM user_revenue
GROUP BY cohort_month
ORDER BY cohort_month DESC;

-- ============================================
-- 4. CHURN ANALYSIS BY COHORT (FIXED)
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_churn_by_cohort` AS
WITH user_activity_gaps AS (
    SELECT 
        user_id,
        DATE(ws.watch_start_time) AS activity_date,
        LAG(DATE(ws.watch_start_time)) OVER (
            PARTITION BY user_id 
            ORDER BY DATE(ws.watch_start_time)
        ) AS prev_activity_date
    FROM `streamflow_analytics.fact_watch_sessions_clean` ws
),
user_churn_flags AS (
    SELECT 
        user_id,
        activity_date,
        prev_activity_date,
        DATE_DIFF(activity_date, prev_activity_date, DAY) AS days_since_last_activity,
        CASE 
            WHEN DATE_DIFF(activity_date, prev_activity_date, DAY) > 30 THEN 1 
            ELSE 0 
        END AS is_churned
    FROM user_activity_gaps
    WHERE prev_activity_date IS NOT NULL
),
cohort_churn AS (
    SELECT 
        DATE_TRUNC(u.signup_date, MONTH) AS cohort_month,
        DATE_TRUNC(cf.activity_date, MONTH) AS churn_month,
        DATE_DIFF(DATE_TRUNC(cf.activity_date, MONTH), DATE_TRUNC(u.signup_date, MONTH), MONTH) AS months_to_churn,
        COUNT(DISTINCT cf.user_id) AS churned_users
    FROM user_churn_flags cf
    JOIN `streamflow_analytics.dim_users_clean` u ON cf.user_id = u.user_id
    WHERE cf.is_churned = 1
    GROUP BY 1, 2, 3
),
cohort_sizes AS (
    SELECT 
        DATE_TRUNC(signup_date, MONTH) AS cohort_month,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM `streamflow_analytics.dim_users_clean`
    GROUP BY 1
)
SELECT 
    cc.cohort_month,
    cc.months_to_churn,
    cc.churned_users,
    cs.cohort_size,
    ROUND(SAFE_DIVIDE(cc.churned_users, cs.cohort_size) * 100, 2) AS churn_rate,
    ROUND(SUM(SAFE_DIVIDE(cc.churned_users, cs.cohort_size) * 100) OVER (
        PARTITION BY cc.cohort_month 
        ORDER BY cc.months_to_churn
    ), 2) AS cumulative_churn_rate
FROM cohort_churn cc
JOIN cohort_sizes cs ON cc.cohort_month = cs.cohort_month
WHERE cc.months_to_churn BETWEEN 1 AND 12
ORDER BY cc.cohort_month DESC, cc.months_to_churn;

-- ============================================
-- 5. COHORT ENGAGEMENT ANALYSIS (FIXED)
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_cohort_engagement` AS
WITH user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC(signup_date, MONTH) AS cohort_month
    FROM `streamflow_analytics.dim_users_clean`
),
cohort_sessions AS (
    SELECT 
        uc.cohort_month,
        DATE_DIFF(DATE_TRUNC(DATE(ws.watch_start_time), MONTH), uc.cohort_month, MONTH) AS months_since_signup,
        ws.user_id,
        ws.session_id,
        ws.watch_duration_minutes,
        ws.completion_rate
    FROM user_cohorts uc
    JOIN `streamflow_analytics.fact_watch_sessions_clean` ws 
        ON uc.user_id = ws.user_id
    WHERE DATE_TRUNC(DATE(ws.watch_start_time), MONTH) >= uc.cohort_month
)
SELECT 
    cohort_month,
    months_since_signup,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(DISTINCT session_id) AS total_sessions,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT session_id), COUNT(DISTINCT user_id)), 2) AS sessions_per_user,
    ROUND(AVG(watch_duration_minutes), 2) AS avg_watch_time_mins,
    ROUND(AVG(completion_rate), 2) AS avg_completion_rate
FROM cohort_sessions
WHERE months_since_signup <= 12
GROUP BY 1, 2
ORDER BY cohort_month DESC, months_since_signup;

-- ============================================
-- 6. WEEKLY RETENTION (MORE GRANULAR) - FIXED
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_weekly_retention` AS
WITH user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC(signup_date, WEEK(MONDAY)) AS cohort_week
    FROM `streamflow_analytics.dim_users_clean`
    WHERE signup_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),
user_activity AS (
    SELECT 
        DISTINCT
        ws.user_id,
        DATE_TRUNC(DATE(ws.watch_start_time), WEEK(MONDAY)) AS activity_week
    FROM `streamflow_analytics.fact_watch_sessions_clean` ws
),
cohort_data AS (
    SELECT 
        uc.cohort_week,
        DATE_DIFF(ua.activity_week, uc.cohort_week, WEEK) AS weeks_since_signup,
        COUNT(DISTINCT uc.user_id) AS active_users
    FROM user_cohorts uc
    LEFT JOIN user_activity ua ON uc.user_id = ua.user_id
    WHERE ua.activity_week >= uc.cohort_week
    GROUP BY 1, 2
),
cohort_sizes AS (
    SELECT 
        cohort_week,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM user_cohorts
    GROUP BY 1
)
SELECT 
    cd.cohort_week,
    cd.weeks_since_signup,
    cd.active_users,
    cs.cohort_size,
    ROUND(SAFE_DIVIDE(cd.active_users, cs.cohort_size) * 100, 2) AS retention_rate
FROM cohort_data cd
JOIN cohort_sizes cs ON cd.cohort_week = cs.cohort_week
WHERE cd.weeks_since_signup BETWEEN 0 AND 12
ORDER BY cd.cohort_week DESC, cd.weeks_since_signup;

-- ============================================
-- 7. REVENUE COHORT ANALYSIS (FIXED)
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_revenue_cohort` AS
WITH user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC(signup_date, MONTH) AS cohort_month
    FROM `streamflow_analytics.dim_users_clean`
),
cohort_revenue AS (
    SELECT 
        uc.cohort_month,
        DATE_DIFF(DATE_TRUNC(s.billing_date, MONTH), uc.cohort_month, MONTH) AS months_since_signup,
        COUNT(DISTINCT uc.user_id) AS paying_users,
        SUM(s.monthly_price) AS cohort_revenue
    FROM user_cohorts uc
    JOIN `streamflow_analytics.fact_subscriptions` s 
        ON uc.user_id = s.user_id
        AND s.payment_status = 'Paid'
    WHERE DATE_TRUNC(s.billing_date, MONTH) >= uc.cohort_month
    GROUP BY 1, 2
),
cohort_sizes AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM user_cohorts
    GROUP BY 1
)
SELECT 
    cr.cohort_month,
    cr.months_since_signup,
    cr.paying_users,
    cs.cohort_size,
    ROUND(SAFE_DIVIDE(cr.paying_users, cs.cohort_size) * 100, 2) AS payer_rate,
    ROUND(cr.cohort_revenue, 2) AS cohort_revenue,
    ROUND(SAFE_DIVIDE(cr.cohort_revenue, cr.paying_users), 2) AS arpu,
    ROUND(SAFE_DIVIDE(cr.cohort_revenue, cs.cohort_size), 2) AS revenue_per_user
FROM cohort_revenue cr
JOIN cohort_sizes cs ON cr.cohort_month = cs.cohort_month
WHERE cr.months_since_signup <= 12
ORDER BY cr.cohort_month DESC, cr.months_since_signup;

-- ============================================
-- 8. COHORT COMPARISON SUMMARY
-- ============================================

CREATE OR REPLACE VIEW `streamflow_analytics.v_cohort_comparison` AS
WITH cohort_performance AS (
    SELECT 
        cohort_month,
        MAX(CASE WHEN months_since_signup = 1 THEN retention_rate END) AS month1_retention,
        MAX(CASE WHEN months_since_signup = 3 THEN retention_rate END) AS month3_retention,
        MAX(CASE WHEN months_since_signup = 6 THEN retention_rate END) AS month6_retention
    FROM `streamflow_analytics.v_cohort_retention`
    GROUP BY cohort_month
)
SELECT 
    CASE 
        WHEN cohort_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) THEN 'Recent Cohorts (Last 6M)'
        ELSE 'Older Cohorts (6M+)'
    END AS cohort_group,
    COUNT(DISTINCT cohort_month) AS number_of_cohorts,
    ROUND(AVG(month1_retention), 2) AS avg_month1_retention,
    ROUND(AVG(month3_retention), 2) AS avg_month3_retention,
    ROUND(AVG(month6_retention), 2) AS avg_month6_retention
FROM cohort_performance
WHERE month1_retention IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================
-- VERIFICATION
-- ============================================
SELECT 
    'Cohort Analysis Complete!' as status,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_cohort_retention`) as retention_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_retention_matrix`) as matrix_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_ltv_by_cohort`) as ltv_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_churn_by_cohort`) as churn_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_cohort_engagement`) as engagement_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_weekly_retention`) as weekly_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_revenue_cohort`) as revenue_cohort_rows,
    (SELECT COUNT(*) FROM `streamflow_analytics.v_cohort_comparison`) as comparison_rows;