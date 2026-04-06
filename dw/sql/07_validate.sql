-- ============================================================
-- INSAF Payroll Intelligence Platform
-- Script 07 — Validation queries for both DWs
--
-- Run after every load.
-- Expected: all "bad_*" columns = 0, known warnings documented.
-- ============================================================

-- ── 1. Row counts ─────────────────────────────────────────────────────────────
SELECT
    'dim_employee'  AS tbl, COUNT(*) AS rows FROM dw.dim_employee  UNION ALL
    SELECT 'dim_grade',     COUNT(*) FROM dw.dim_grade             UNION ALL
    SELECT 'dim_nature',    COUNT(*) FROM dw.dim_nature            UNION ALL
    SELECT 'dim_organisme', COUNT(*) FROM dw.dim_organisme         UNION ALL
    SELECT 'dim_region',    COUNT(*) FROM dw.dim_region            UNION ALL
    SELECT 'dim_temps',     COUNT(*) FROM dw.dim_temps             UNION ALL
    SELECT 'dim_indemnite', COUNT(*) FROM dw.dim_indemnite         UNION ALL
    SELECT 'fact_paie',     COUNT(*) FROM dw.fact_paie             UNION ALL
    SELECT 'fact_indem',    COUNT(*) FROM dw.fact_indem
ORDER BY tbl;


-- ── 2. Grain violations (must be 0 after load) ────────────────────────────────
SELECT
    'fact_paie grain duplicates' AS check_name,
    COUNT(*) AS count
FROM (
    SELECT employee_sk, time_sk, pa_type, COUNT(*) c
    FROM dw.fact_paie GROUP BY 1,2,3 HAVING COUNT(*) > 1
) x
UNION ALL
SELECT
    'fact_indem grain duplicates',
    COUNT(*)
FROM (
    SELECT employee_sk, time_sk, pa_type, COUNT(*) c
    FROM dw.fact_indem GROUP BY 1,2,3 HAVING COUNT(*) > 1
) x;


-- ── 3. Measure sanity — DW1 ───────────────────────────────────────────────────
SELECT
    COUNT(*)                                                   AS total_paie,
    COUNT(*) FILTER (WHERE m_netpay IS NULL)                   AS null_netpay,
    COUNT(*) FILTER (WHERE m_salbrut IS NULL)                  AS null_salbrut,
    COUNT(*) FILTER (WHERE m_netpay < 0)                       AS negative_netpay,
    COUNT(*) FILTER (WHERE m_salbrut < 0)                      AS negative_salbrut,
    COUNT(*) FILTER (WHERE m_netpay > m_salbrut
                       AND m_salbrut > 0)                      AS netpay_exceeds_gross
FROM dw.fact_paie;


-- ── 4. Measure sanity — DW2 ───────────────────────────────────────────────────
SELECT
    COUNT(*)                                                   AS total_indem,
    COUNT(*) FILTER (WHERE m_netpay IS NULL)                   AS null_netpay,
    COUNT(*) FILTER (WHERE m_netpay < 0)                       AS negative_netpay
FROM dw.fact_indem;


-- ── 5. Unknown member usage — DW1 ────────────────────────────────────────────
-- pct_region_unknown expected to be high (~99%) — documented limitation.
SELECT
    ROUND(100.0 * SUM(CASE WHEN employee_sk  = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_employee,
    ROUND(100.0 * SUM(CASE WHEN grade_sk     = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_grade,
    ROUND(100.0 * SUM(CASE WHEN nature_sk    = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_nature,
    ROUND(100.0 * SUM(CASE WHEN organisme_sk = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_org,
    ROUND(100.0 * SUM(CASE WHEN region_sk    = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_region
FROM dw.fact_paie;


-- ── 6. Unknown member usage — DW2 ────────────────────────────────────────────
SELECT
    ROUND(100.0 * SUM(CASE WHEN employee_sk  = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_employee,
    ROUND(100.0 * SUM(CASE WHEN grade_sk     = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_grade,
    ROUND(100.0 * SUM(CASE WHEN nature_sk    = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_nature,
    ROUND(100.0 * SUM(CASE WHEN organisme_sk = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_org,
    ROUND(100.0 * SUM(CASE WHEN region_sk    = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_region,
    ROUND(100.0 * SUM(CASE WHEN indemnite_sk = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_unknown_indemnite
FROM dw.fact_indem;


-- ── 7. DQ flag summary ───────────────────────────────────────────────────────
SELECT
    'DW1_paie' AS dw,
    ROUND(100.0 * SUM(CASE WHEN dq_grade_matched  THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_grade_ok,
    ROUND(100.0 * SUM(CASE WHEN dq_nature_matched THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_nature_ok,
    ROUND(100.0 * SUM(CASE WHEN dq_org_matched    THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_org_ok,
    ROUND(100.0 * SUM(CASE WHEN dq_region_matched THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_region_ok,
    ROUND(100.0 * SUM(CASE WHEN dq_has_issues     THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS pct_has_issues
FROM dw.fact_paie
UNION ALL
SELECT
    'DW2_indem',
    ROUND(100.0 * SUM(CASE WHEN dq_grade_matched  THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2),
    ROUND(100.0 * SUM(CASE WHEN dq_nature_matched THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2),
    ROUND(100.0 * SUM(CASE WHEN dq_org_matched    THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2),
    ROUND(100.0 * SUM(CASE WHEN dq_region_matched THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2),
    ROUND(100.0 * SUM(CASE WHEN dq_has_issues     THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2)
FROM dw.fact_indem;


-- ── 8. Sample KPI check — DW1 ────────────────────────────────────────────────
SELECT
    dt.year_num,
    dt.month_num,
    COUNT(*)                      AS employee_count,
    SUM(fp.m_netpay)              AS total_net_pay,
    AVG(fp.m_netpay)              AS avg_net_pay,
    MIN(fp.m_netpay)              AS min_net_pay,
    MAX(fp.m_netpay)              AS max_net_pay
FROM dw.fact_paie fp
JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
WHERE fp.employee_sk <> 0
  AND fp.m_netpay IS NOT NULL
GROUP BY dt.year_num, dt.month_num
ORDER BY dt.year_num, dt.month_num;


-- ── 9. Sample KPI check — DW2 ────────────────────────────────────────────────
SELECT
    dt.year_num,
    dt.month_num,
    COUNT(*)           AS employee_count,
    SUM(fi.m_netpay)   AS total_indem_pay,
    AVG(fi.m_netpay)   AS avg_indem_pay
FROM dw.fact_indem fi
JOIN dw.dim_temps dt ON dt.time_sk = fi.time_sk
WHERE fi.employee_sk <> 0
  AND fi.m_netpay IS NOT NULL
GROUP BY dt.year_num, dt.month_num
ORDER BY dt.year_num, dt.month_num;
