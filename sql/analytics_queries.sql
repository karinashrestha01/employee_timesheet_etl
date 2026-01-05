
-- 1. Active Headcount Over Time

-- Number of employees actively employed on a given date
-- Tracks workforce size and identifies hiring/attrition trends

WITH date_series AS (
    SELECT DISTINCT work_date AS report_date
    FROM dim_date
    ORDER BY work_date
)
SELECT 
    ds.report_date,
    COUNT(DISTINCT e.employee_key) AS active_headcount
FROM date_series ds
CROSS JOIN dim_employee e
WHERE e.hire_date <= ds.report_date
  AND (e.termination_date IS NULL OR e.termination_date > ds.report_date)
GROUP BY ds.report_date
ORDER BY ds.report_date;

-- Simplified version for current active headcount
SELECT 
    COUNT(*) AS current_active_headcount
FROM dim_employee
WHERE is_active = 1;



-- 2. Turnover Trend

-- Measure of employee terminations across time periods (monthly/quarterly)
-- Monitors organizational stability and highlights peak turnover periods

-- Monthly turnover
SELECT 
    DATE_TRUNC('month', termination_date) AS turnover_month,
    COUNT(*) AS terminations,
    ROUND(COUNT(*) * 100.0 / NULLIF(
        (SELECT COUNT(*) FROM dim_employee WHERE hire_date <= DATE_TRUNC('month', termination_date)), 
        0
    ), 2) AS turnover_rate_pct
FROM dim_employee
WHERE termination_date IS NOT NULL
GROUP BY DATE_TRUNC('month', termination_date)
ORDER BY turnover_month;

-- Quarterly turnover
SELECT 
    DATE_TRUNC('quarter', termination_date) AS turnover_quarter,
    COUNT(*) AS terminations,
    ROUND(COUNT(*) * 100.0 / NULLIF(
        (SELECT COUNT(*) FROM dim_employee WHERE hire_date <= DATE_TRUNC('quarter', termination_date)), 
        0
    ), 2) AS turnover_rate_pct
FROM dim_employee
WHERE termination_date IS NOT NULL
GROUP BY DATE_TRUNC('quarter', termination_date)
ORDER BY turnover_quarter;



-- 3. Average Tenure by Department

-- Average employment duration of staff within each department
-- Evaluates retention effectiveness and workforce experience per department

SELECT 
    d.department_name,
    d.department_id,
    COUNT(e.employee_key) AS total_employees,
    ROUND(AVG(
        CASE 
            WHEN e.termination_date IS NULL 
            THEN EXTRACT(EPOCH FROM (CURRENT_DATE - e.hire_date)) / 86400 / 365.25
            ELSE EXTRACT(EPOCH FROM (e.termination_date - e.hire_date)) / 86400 / 365.25
        END
    ), 2) AS avg_tenure_years,
    ROUND(AVG(
        CASE 
            WHEN e.termination_date IS NULL 
            THEN EXTRACT(EPOCH FROM (CURRENT_DATE - e.hire_date)) / 86400 / 30.44
            ELSE EXTRACT(EPOCH FROM (e.termination_date - e.hire_date)) / 86400 / 30.44
        END
    ), 2) AS avg_tenure_months
FROM dim_employee e
JOIN dim_department d ON e.department_key = d.department_key
GROUP BY d.department_name, d.department_id
ORDER BY avg_tenure_years DESC;



-- 4. Average Working Hours per Employee

-- Mean number of hours worked per day or per week by each employee
-- Indicates productivity levels and workload balance

-- Daily average per employee
SELECT 
    e.employee_id,
    e.first_name || ' ' || e.last_name AS employee_name,
    d.department_name,
    COUNT(DISTINCT f.work_date) AS total_work_days,
    ROUND(AVG(f.hours_worked), 2) AS avg_hours_per_day,
    ROUND(SUM(f.hours_worked), 2) AS total_hours_worked
FROM fact_timesheet f
JOIN dim_employee e ON f.employee_key = e.employee_key
JOIN dim_department d ON e.department_key = d.department_key
GROUP BY e.employee_id, e.first_name, e.last_name, d.department_name
ORDER BY avg_hours_per_day DESC;

-- Weekly average per employee
SELECT 
    e.employee_id,
    e.first_name || ' ' || e.last_name AS employee_name,
    d.department_name,
    DATE_TRUNC('week', f.work_date) AS work_week,
    ROUND(SUM(f.hours_worked), 2) AS weekly_hours,
    ROUND(AVG(f.hours_worked), 2) AS avg_daily_hours
FROM fact_timesheet f
JOIN dim_employee e ON f.employee_key = e.employee_key
JOIN dim_department d ON e.department_key = d.department_key
GROUP BY e.employee_id, e.first_name, e.last_name, d.department_name, DATE_TRUNC('week', f.work_date)
ORDER BY work_week DESC, weekly_hours DESC;



-- 5. Late Arrival Frequency

-- Number of times employee clocked in later than scheduled start time
-- Grace period: +/- 5 minutes
-- Assesses punctuality and discipline

SELECT 
    e.employee_id,
    e.first_name || ' ' || e.last_name AS employee_name,
    d.department_name,
    COUNT(*) AS total_work_days,
    SUM(CASE 
        WHEN f.late_arrival_minutes > 5 THEN 1 
        ELSE 0 
    END) AS late_arrival_count,
    ROUND(AVG(CASE 
        WHEN f.late_arrival_minutes > 5 THEN f.late_arrival_minutes 
        ELSE NULL 
    END), 2) AS avg_late_minutes,
    ROUND(
        SUM(CASE WHEN f.late_arrival_minutes > 5 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 
        2
    ) AS late_arrival_rate_pct
FROM fact_timesheet f
JOIN dim_employee e ON f.employee_key = e.employee_key
JOIN dim_department d ON e.department_key = d.department_key
WHERE f.punch_in IS NOT NULL 
  AND f.scheduled_start_datetime IS NOT NULL
GROUP BY e.employee_id, e.first_name, e.last_name, d.department_name
HAVING SUM(CASE WHEN f.late_arrival_minutes > 5 THEN 1 ELSE 0 END) > 0
ORDER BY late_arrival_count DESC;



-- 6. Early Departure Count

-- Number of days employees left earlier than expected shift end time
-- Grace period: +/- 5 minutes
-- Highlights attendance irregularities and potential productivity loss

SELECT 
    e.employee_id,
    e.first_name || ' ' || e.last_name AS employee_name,
    d.department_name,
    COUNT(*) AS total_work_days,
    SUM(CASE 
        WHEN f.early_departure_minutes > 5 THEN 1 
        ELSE 0 
    END) AS early_departure_count,
    ROUND(AVG(CASE 
        WHEN f.early_departure_minutes > 5 THEN f.early_departure_minutes 
        ELSE NULL 
    END), 2) AS avg_early_minutes,
    ROUND(
        SUM(CASE WHEN f.early_departure_minutes > 5 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 
        2
    ) AS early_departure_rate_pct
FROM fact_timesheet f
JOIN dim_employee e ON f.employee_key = e.employee_key
JOIN dim_department d ON e.department_key = d.department_key
WHERE f.punch_out IS NOT NULL 
  AND f.scheduled_end_datetime IS NOT NULL
GROUP BY e.employee_id, e.first_name, e.last_name, d.department_name
HAVING SUM(CASE WHEN f.early_departure_minutes > 5 THEN 1 ELSE 0 END) > 0
ORDER BY early_departure_count DESC;



-- 7. Total Overtime Count

-- Total workdays/hours where employees exceeded standard shift duration
-- Grace period: +/- 5 minutes (considered overtime if > 5 min over scheduled)
-- Highlights workload pressure, potential fatigue, and need for optimization

SELECT 
    e.employee_id,
    e.first_name || ' ' || e.last_name AS employee_name,
    d.department_name,
    COUNT(*) AS total_work_days,
    SUM(CASE 
        WHEN f.hours_worked > f.hours_scheduled + (5.0/60.0) THEN 1 
        ELSE 0 
    END) AS overtime_days,
    ROUND(SUM(CASE 
        WHEN f.hours_worked > f.hours_scheduled + (5.0/60.0) 
        THEN f.hours_worked - f.hours_scheduled 
        ELSE 0 
    END), 2) AS total_overtime_hours,
    ROUND(AVG(CASE 
        WHEN f.hours_worked > f.hours_scheduled + (5.0/60.0) 
        THEN f.hours_worked - f.hours_scheduled 
        ELSE NULL 
    END), 2) AS avg_overtime_hours_per_ot_day,
    ROUND(
        SUM(CASE WHEN f.hours_worked > f.hours_scheduled + (5.0/60.0) THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 
        2
    ) AS overtime_frequency_pct
FROM fact_timesheet f
JOIN dim_employee e ON f.employee_key = e.employee_key
JOIN dim_department d ON e.department_key = d.department_key
WHERE f.hours_scheduled > 0  -- Only consider days with scheduled hours
GROUP BY e.employee_id, e.first_name, e.last_name, d.department_name
HAVING SUM(CASE WHEN f.hours_worked > f.hours_scheduled + (5.0/60.0) THEN 1 ELSE 0 END) > 0
ORDER BY total_overtime_hours DESC;



-- 8. Rolling Average Working Hours

-- Moving average of working hours across a defined recent time window (e.g., 7 days, 30 days)
-- Detects trends such as increasing overtime or reduced productivity

-- 7-day rolling average per employee
SELECT 
    e.employee_id,
    e.first_name || ' ' || e.last_name AS employee_name,
    f.work_date,
    f.hours_worked AS daily_hours,
    ROUND(AVG(f.hours_worked) OVER (
        PARTITION BY e.employee_key 
        ORDER BY f.work_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_7day_avg_hours,
    ROUND(AVG(f.hours_worked) OVER (
        PARTITION BY e.employee_key 
        ORDER BY f.work_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_30day_avg_hours
FROM fact_timesheet f
JOIN dim_employee e ON f.employee_key = e.employee_key
ORDER BY e.employee_id, f.work_date DESC;

-- Department-level rolling average
SELECT 
    d.department_name,
    dt.work_date,
    ROUND(AVG(f.hours_worked), 2) AS daily_avg_hours,
    ROUND(AVG(AVG(f.hours_worked)) OVER (
        PARTITION BY d.department_key 
        ORDER BY dt.work_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_7day_avg_hours,
    ROUND(AVG(AVG(f.hours_worked)) OVER (
        PARTITION BY d.department_key 
        ORDER BY dt.work_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_30day_avg_hours
FROM fact_timesheet f
JOIN dim_employee e ON f.employee_key = e.employee_key
JOIN dim_department d ON e.department_key = d.department_key
JOIN dim_date dt ON f.work_date = dt.work_date
GROUP BY d.department_key, d.department_name, dt.work_date
ORDER BY d.department_name, dt.work_date DESC;



-- 9. Early Attrition Rate

-- Proportion of employees who leave within the first few months of joining
-- Identifies issues with recruitment, onboarding, or job satisfaction

-- Employees who left within first 3, 6, and 12 months
WITH tenure_at_termination AS (
    SELECT 
        employee_key,
        employee_id,
        first_name || ' ' || last_name AS employee_name,
        hire_date,
        termination_date,
        EXTRACT(EPOCH FROM (termination_date - hire_date)) / 86400 / 30.44 AS tenure_months
    FROM dim_employee
    WHERE termination_date IS NOT NULL
)
SELECT 
    COUNT(*) AS total_terminations,
    SUM(CASE WHEN tenure_months <= 3 THEN 1 ELSE 0 END) AS left_within_3_months,
    SUM(CASE WHEN tenure_months <= 6 THEN 1 ELSE 0 END) AS left_within_6_months,
    SUM(CASE WHEN tenure_months <= 12 THEN 1 ELSE 0 END) AS left_within_12_months,
    ROUND(SUM(CASE WHEN tenure_months <= 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS early_attrition_3mo_pct,
    ROUND(SUM(CASE WHEN tenure_months <= 6 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS early_attrition_6mo_pct,
    ROUND(SUM(CASE WHEN tenure_months <= 12 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS early_attrition_12mo_pct
FROM tenure_at_termination;

-- Detailed list of early leavers (within 6 months)
SELECT 
    e.employee_id,
    e.first_name || ' ' || e.last_name AS employee_name,
    d.department_name,
    e.job_title,
    e.hire_date,
    e.termination_date,
    ROUND(EXTRACT(EPOCH FROM (e.termination_date - e.hire_date)) / 86400 / 30.44, 1) AS tenure_months,
    ROUND(EXTRACT(EPOCH FROM (e.termination_date - e.hire_date)) / 86400, 0) AS tenure_days
FROM dim_employee e
JOIN dim_department d ON e.department_key = d.department_key
WHERE e.termination_date IS NOT NULL
  AND EXTRACT(EPOCH FROM (e.termination_date - e.hire_date)) / 86400 / 30.44 <= 6
ORDER BY tenure_months;

-- Early attrition by department
SELECT 
    d.department_name,
    COUNT(*) AS total_terminations,
    SUM(CASE 
        WHEN EXTRACT(EPOCH FROM (e.termination_date - e.hire_date)) / 86400 / 30.44 <= 6 
        THEN 1 ELSE 0 
    END) AS early_leavers_6mo,
    ROUND(
        SUM(CASE 
            WHEN EXTRACT(EPOCH FROM (e.termination_date - e.hire_date)) / 86400 / 30.44 <= 6 
            THEN 1 ELSE 0 
        END) * 100.0 / COUNT(*), 
        2
    ) AS early_attrition_rate_pct
FROM dim_employee e
JOIN dim_department d ON e.department_key = d.department_key
WHERE e.termination_date IS NOT NULL
GROUP BY d.department_name
ORDER BY early_attrition_rate_pct DESC;


SELECT 
    -- Headcount metrics
    (SELECT COUNT(*) FROM dim_employee WHERE is_active = 1) AS current_active_headcount,
    (SELECT COUNT(*) FROM dim_employee WHERE termination_date IS NOT NULL) AS total_terminations,
    
    -- Turnover metrics
    (SELECT COUNT(*) 
     FROM dim_employee 
     WHERE termination_date >= DATE_TRUNC('month', CURRENT_DATE)
    ) AS terminations_this_month,
    
    -- Average tenure
    (SELECT ROUND(AVG(
        CASE 
            WHEN termination_date IS NULL 
            THEN EXTRACT(EPOCH FROM (CURRENT_DATE - hire_date)) / 86400 / 365.25
            ELSE EXTRACT(EPOCH FROM (termination_date - hire_date)) / 86400 / 365.25
        END
    ), 2) FROM dim_employee) AS avg_tenure_years,
    
    -- Productivity metrics
    (SELECT ROUND(AVG(hours_worked), 2) FROM fact_timesheet) AS avg_daily_hours,
    (SELECT ROUND(SUM(hours_worked), 2) FROM fact_timesheet) AS total_hours_worked,
    
    -- Attendance metrics
    (SELECT COUNT(*) FROM fact_timesheet WHERE late_arrival_minutes > 5) AS total_late_arrivals,
    (SELECT COUNT(*) FROM fact_timesheet WHERE early_departure_minutes > 5) AS total_early_departures,
    
    -- Overtime metrics
    (SELECT COUNT(*) 
     FROM fact_timesheet 
     WHERE hours_worked > hours_scheduled + (5.0/60.0) AND hours_scheduled > 0
    ) AS total_overtime_days;
