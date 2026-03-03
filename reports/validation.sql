SELECT 'staging.stg_paie2015' AS table_name, COUNT(*) AS row_count FROM staging.stg_paie2015
UNION ALL
SELECT 'staging.stg_ind2015', COUNT(*) FROM staging.stg_ind2015
UNION ALL
SELECT 'staging.stg_grade', COUNT(*) FROM staging.stg_grade
UNION ALL
SELECT 'staging.stg_nature', COUNT(*) FROM staging.stg_nature
UNION ALL
SELECT 'staging.stg_region', COUNT(*) FROM staging.stg_region
UNION ALL
SELECT 'staging.stg_organisme', COUNT(*) FROM staging.stg_organisme
UNION ALL
SELECT 'staging.stg_indem_def', COUNT(*) FROM staging.stg_indem_def
UNION ALL
SELECT 'public.dim_employee', COUNT(*) FROM public.dim_employee
UNION ALL
SELECT 'public.dim_temps', COUNT(*) FROM public.dim_temps
UNION ALL
SELECT 'public.dim_grade', COUNT(*) FROM public.dim_grade
UNION ALL
SELECT 'public.dim_nature', COUNT(*) FROM public.dim_nature
UNION ALL
SELECT 'public.dim_region', COUNT(*) FROM public.dim_region
UNION ALL
SELECT 'public.dim_organisme', COUNT(*) FROM public.dim_organisme
UNION ALL
SELECT 'public.dim_indemnite', COUNT(*) FROM public.dim_indemnite
UNION ALL
SELECT 'public.fact_paie', COUNT(*) FROM public.fact_paie
UNION ALL
SELECT 'public.fact_indemnite', COUNT(*) FROM public.fact_indemnite
ORDER BY table_name;

SELECT
    pa_mat, pa_annee, pa_mois, pa_type, pa_sec, pa_codmin, pa_dire, pa_article, pa_parag,
    COUNT(*) AS duplicate_count
FROM public.fact_paie
GROUP BY pa_mat, pa_annee, pa_mois, pa_type, pa_sec, pa_codmin, pa_dire, pa_article, pa_parag
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 100;

SELECT
    pa_mat, pa_annee, pa_mois, pa_type, pa_sec, pa_codmin, pa_dire, pa_article, pa_parag,
    COUNT(*) AS duplicate_count
FROM public.fact_indemnite
GROUP BY pa_mat, pa_annee, pa_mois, pa_type, pa_sec, pa_codmin, pa_dire, pa_article, pa_parag
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 100;

SELECT
    COUNT(*) FILTER (WHERE employee_sk IS NULL) AS null_employee_sk,
    COUNT(*) FILTER (WHERE time_sk IS NULL) AS null_time_sk,
    COUNT(*) FILTER (WHERE pa_mat IS NULL OR pa_mat = '') AS null_pa_mat,
    COUNT(*) FILTER (WHERE pa_annee IS NULL) AS null_pa_annee,
    COUNT(*) FILTER (WHERE pa_mois IS NULL) AS null_pa_mois
FROM public.fact_paie;

SELECT
    COUNT(*) FILTER (WHERE employee_sk IS NULL) AS null_employee_sk,
    COUNT(*) FILTER (WHERE time_sk IS NULL) AS null_time_sk,
    COUNT(*) FILTER (WHERE pa_mat IS NULL OR pa_mat = '') AS null_pa_mat,
    COUNT(*) FILTER (WHERE pa_annee IS NULL) AS null_pa_annee,
    COUNT(*) FILTER (WHERE pa_mois IS NULL) AS null_pa_mois
FROM public.fact_indemnite;

SELECT COUNT(*) AS invalid_netpay_gt_salbrut
FROM public.fact_paie
WHERE pa_netpay IS NOT NULL
  AND pa_salbrut IS NOT NULL
  AND pa_netpay > pa_salbrut;

SELECT COUNT(*) AS invalid_netpay_gt_salbrut
FROM public.fact_indemnite
WHERE pa_netpay IS NOT NULL
  AND pa_salbrut IS NOT NULL
  AND pa_netpay > pa_salbrut;

SELECT
    COALESCE(o.codetab, 'UNKNOWN') AS ministry_code,
    ROUND(SUM(COALESCE(f.pa_netpay, 0))::numeric, 2) AS total_netpay
FROM public.fact_paie f
LEFT JOIN public.dim_organisme o ON o.organisme_sk = f.organisme_sk
GROUP BY COALESCE(o.codetab, 'UNKNOWN')
ORDER BY total_netpay DESC
LIMIT 10;
