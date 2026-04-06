-- ============================================================
-- INSAF Payroll Intelligence Platform
-- Script 06 — Load both fact tables from clean JSONL
--
-- \copy commands (run in psql before executing this script):
--   \copy stg.fact_paie_raw  FROM '/path/fact_paie.jsonl'
--   \copy stg.fact_indem_raw FROM '/path/fact_indem.jsonl'
-- ============================================================

-- ── Staging tables ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stg.fact_paie_raw  (raw JSONB);
CREATE TABLE IF NOT EXISTS stg.fact_indem_raw (raw JSONB);

TRUNCATE stg.fact_paie_raw, stg.fact_indem_raw;

-- ════════════════════════════════════════════════════════════════
-- Helper macro: cast a JSONB text field to NUMERIC safely.
-- Returns NULL if the field is missing, empty, or not numeric.
-- ════════════════════════════════════════════════════════════════
-- (Inline below as CASE expressions — no function needed.)


-- ── Load fact_paie (DW1) ──────────────────────────────────────────────────────
INSERT INTO dw.fact_paie (
    employee_sk, time_sk, pa_type,
    grade_sk, nature_sk, organisme_sk, region_sk,
    pa_eche, pa_sitfam, pa_loca_raw,
    m_salimp, m_salnimp, m_salbrut, m_brutcnr, m_netord, m_netpay,
    m_cpe, m_retrait, m_cps, m_capdeces,
    m_avkm, m_avlog,
    m_rapimp, m_rapni, m_sub, m_sps, m_spl, m_rapsalb,
    dq_grade_matched, dq_nature_matched, dq_org_matched, dq_region_matched,
    dq_has_issues, dq_issue_count,
    run_id, source_file
)
SELECT
    COALESCE(de.employee_sk,  0),
    COALESCE(dt.time_sk,      0),
    COALESCE(NULLIF(f.raw->>'pa_type',  ''), '1'),
    COALESCE(dg.grade_sk,     0),
    COALESCE(dn.nature_sk,    0),
    COALESCE(do_.organisme_sk, 0),
    COALESCE(dr.region_sk,    0),
    CASE WHEN f.raw->>'pa_eche' ~ '^\d+$' THEN (f.raw->>'pa_eche')::SMALLINT END,
    NULLIF(f.raw->>'pa_sitfam', ''),
    NULLIF(f.raw->>'pa_loca',   ''),
    -- Measures: NULL for absent, cast to numeric for present values
    CASE WHEN f.raw->>'pa_salimp'  ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_salimp')::NUMERIC  END,
    CASE WHEN f.raw->>'pa_salnimp' ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_salnimp')::NUMERIC END,
    CASE WHEN f.raw->>'pa_salbrut' ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_salbrut')::NUMERIC END,
    CASE WHEN f.raw->>'pa_brutcnr' ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_brutcnr')::NUMERIC END,
    CASE WHEN f.raw->>'pa_netord'  ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_netord')::NUMERIC  END,
    CASE WHEN f.raw->>'pa_netpay'  ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_netpay')::NUMERIC  END,
    CASE WHEN f.raw->>'pa_cpe'     ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_cpe')::NUMERIC     END,
    CASE WHEN f.raw->>'pa_retrait' ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_retrait')::NUMERIC END,
    CASE WHEN f.raw->>'pa_cps'     ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_cps')::NUMERIC     END,
    CASE WHEN f.raw->>'pa_capdeces'~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_capdeces')::NUMERIC END,
    CASE WHEN f.raw->>'pa_avkm'    ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_avkm')::NUMERIC    END,
    CASE WHEN f.raw->>'pa_avlog'   ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_avlog')::NUMERIC   END,
    CASE WHEN f.raw->>'pa_rapimp'  ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_rapimp')::NUMERIC  END,
    CASE WHEN f.raw->>'pa_rapni'   ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_rapni')::NUMERIC   END,
    CASE WHEN f.raw->>'pa_sub'     ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_sub')::NUMERIC     END,
    CASE WHEN f.raw->>'pa_sps'     ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_sps')::NUMERIC     END,
    CASE WHEN f.raw->>'pa_spl'     ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_spl')::NUMERIC     END,
    CASE WHEN f.raw->>'pa_rapsalb' ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_rapsalb')::NUMERIC END,
    -- DQ flags
    (f.raw->>'dq_grade_matched')::BOOLEAN,
    (f.raw->>'dq_nature_matched')::BOOLEAN,
    (f.raw->>'dq_org_matched')::BOOLEAN,
    (f.raw->>'dq_region_matched')::BOOLEAN,
    COALESCE((f.raw->>'dq_has_issues')::BOOLEAN, FALSE),
    COALESCE(NULLIF(f.raw->>'dq_issue_count','')::SMALLINT, 0),
    COALESCE(f.raw->>'run_id',      'unknown'),
    COALESCE(f.raw->>'source_file', 'unknown')
FROM stg.fact_paie_raw f
LEFT JOIN dw.dim_employee  de  ON de.employee_id  = f.raw->>'pa_mat'
LEFT JOIN dw.dim_temps     dt  ON dt.year_num      = (f.raw->>'pa_annee')::INT
                               AND dt.month_num    = (f.raw->>'pa_mois')::INT
LEFT JOIN dw.dim_grade     dg  ON dg.grade_code    = f.raw->>'pa_grd'
LEFT JOIN dw.dim_nature    dn  ON dn.nature_code   = f.raw->>'pa_natu'
LEFT JOIN dw.dim_organisme do_ ON do_.codetab      = f.raw->>'pa_codmin'
                               AND do_.dire        = f.raw->>'pa_dire'
LEFT JOIN dw.dim_region    dr  ON dr.coddep        = f.raw->>'pa_codmin'
ON CONFLICT (employee_sk, time_sk, pa_type) DO UPDATE SET
    grade_sk       = EXCLUDED.grade_sk,
    nature_sk      = EXCLUDED.nature_sk,
    organisme_sk   = EXCLUDED.organisme_sk,
    region_sk      = EXCLUDED.region_sk,
    m_netpay       = EXCLUDED.m_netpay,
    m_salbrut      = EXCLUDED.m_salbrut,
    dq_has_issues  = EXCLUDED.dq_has_issues,
    load_ts        = NOW();


-- ── Load fact_indem (DW2) ──────────────────────────────────────────────────────
INSERT INTO dw.fact_indem (
    employee_sk, time_sk, pa_type,
    grade_sk, nature_sk, organisme_sk, region_sk, indemnite_sk,
    pa_eche, pa_sitfam, pa_loca_raw,
    m_salimp, m_salnimp, m_salbrut, m_brutcnr, m_netord, m_netpay,
    m_cpe, m_retrait, m_cps, m_capdeces,
    m_avkm, m_avlog,
    m_rapimp, m_rapni, m_sub, m_sps, m_spl, m_rapsalb,
    dq_grade_matched, dq_nature_matched, dq_org_matched, dq_region_matched,
    dq_has_issues, dq_issue_count,
    run_id, source_file
)
SELECT
    COALESCE(de.employee_sk,  0),
    COALESCE(dt.time_sk,      0),
    COALESCE(NULLIF(f.raw->>'pa_type',  ''), '3'),
    COALESCE(dg.grade_sk,     0),
    COALESCE(dn.nature_sk,    0),
    COALESCE(do_.organisme_sk, 0),
    COALESCE(dr.region_sk,    0),
    0,   -- indemnite_sk = 0 (Unknown) until explicit code column available
    CASE WHEN f.raw->>'pa_eche' ~ '^\d+$' THEN (f.raw->>'pa_eche')::SMALLINT END,
    NULLIF(f.raw->>'pa_sitfam', ''),
    NULLIF(f.raw->>'pa_loca',   ''),
    CASE WHEN f.raw->>'pa_salimp'  ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_salimp')::NUMERIC  END,
    CASE WHEN f.raw->>'pa_salnimp' ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_salnimp')::NUMERIC END,
    CASE WHEN f.raw->>'pa_salbrut' ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_salbrut')::NUMERIC END,
    CASE WHEN f.raw->>'pa_brutcnr' ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_brutcnr')::NUMERIC END,
    CASE WHEN f.raw->>'pa_netord'  ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_netord')::NUMERIC  END,
    CASE WHEN f.raw->>'pa_netpay'  ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_netpay')::NUMERIC  END,
    CASE WHEN f.raw->>'pa_cpe'     ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_cpe')::NUMERIC     END,
    CASE WHEN f.raw->>'pa_retrait' ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_retrait')::NUMERIC END,
    CASE WHEN f.raw->>'pa_cps'     ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_cps')::NUMERIC     END,
    CASE WHEN f.raw->>'pa_capdeces'~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_capdeces')::NUMERIC END,
    CASE WHEN f.raw->>'pa_avkm'    ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_avkm')::NUMERIC    END,
    CASE WHEN f.raw->>'pa_avlog'   ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_avlog')::NUMERIC   END,
    CASE WHEN f.raw->>'pa_rapimp'  ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_rapimp')::NUMERIC  END,
    CASE WHEN f.raw->>'pa_rapni'   ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_rapni')::NUMERIC   END,
    CASE WHEN f.raw->>'pa_sub'     ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_sub')::NUMERIC     END,
    CASE WHEN f.raw->>'pa_sps'     ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_sps')::NUMERIC     END,
    CASE WHEN f.raw->>'pa_spl'     ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_spl')::NUMERIC     END,
    CASE WHEN f.raw->>'pa_rapsalb' ~ '^-?\d+(\.\d+)?$' THEN (f.raw->>'pa_rapsalb')::NUMERIC END,
    (f.raw->>'dq_grade_matched')::BOOLEAN,
    (f.raw->>'dq_nature_matched')::BOOLEAN,
    (f.raw->>'dq_org_matched')::BOOLEAN,
    (f.raw->>'dq_region_matched')::BOOLEAN,
    COALESCE((f.raw->>'dq_has_issues')::BOOLEAN, FALSE),
    COALESCE(NULLIF(f.raw->>'dq_issue_count','')::SMALLINT, 0),
    COALESCE(f.raw->>'run_id',      'unknown'),
    COALESCE(f.raw->>'source_file', 'unknown')
FROM stg.fact_indem_raw f
LEFT JOIN dw.dim_employee  de  ON de.employee_id  = f.raw->>'pa_mat'
LEFT JOIN dw.dim_temps     dt  ON dt.year_num      = (f.raw->>'pa_annee')::INT
                               AND dt.month_num    = (f.raw->>'pa_mois')::INT
LEFT JOIN dw.dim_grade     dg  ON dg.grade_code    = f.raw->>'pa_grd'
LEFT JOIN dw.dim_nature    dn  ON dn.nature_code   = f.raw->>'pa_natu'
LEFT JOIN dw.dim_organisme do_ ON do_.codetab      = f.raw->>'pa_codmin'
                               AND do_.dire        = f.raw->>'pa_dire'
LEFT JOIN dw.dim_region    dr  ON dr.coddep        = f.raw->>'pa_codmin'
ON CONFLICT (employee_sk, time_sk, pa_type) DO UPDATE SET
    grade_sk       = EXCLUDED.grade_sk,
    nature_sk      = EXCLUDED.nature_sk,
    organisme_sk   = EXCLUDED.organisme_sk,
    region_sk      = EXCLUDED.region_sk,
    m_netpay       = EXCLUDED.m_netpay,
    m_salbrut      = EXCLUDED.m_salbrut,
    dq_has_issues  = EXCLUDED.dq_has_issues,
    load_ts        = NOW();
