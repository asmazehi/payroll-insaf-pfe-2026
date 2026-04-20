-- ============================================================
-- INSAF Payroll Intelligence Platform
-- Script 08 — BI Views (DW2 indemnity only)
--
-- DW1 (Paie) uses the original tables directly — no views needed.
--
-- DW2 (Indemnities) needs views because:
--   - fact_indem must be filtered to paie-matched employees only (emp_sk != 0)
--   - dim_* views provide clean column aliases for Power BI
--
-- In Power BI:
--   DW1 → connect to: dim_employee, dim_grade, dim_nature,
--                      dim_organisme, dim_region, dim_temps, fact_paie
--   DW2 → connect to: vw_dw2_fact_indem + vw_dw2_dim_*
-- ============================================================

SET search_path = dw, public;

-- ════════════════════════════════════════════════════════════
-- DW2 — Indemnity Star Schema
-- ════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW dw.vw_dw2_fact_indem AS
SELECT
    fi.employee_sk,
    fi.time_sk,
    fi.grade_sk,
    fi.nature_sk,
    fi.organisme_sk,
    fi.region_sk,
    fi.indemnite_sk,
    fi.pa_eche,
    fi.pa_sitfam,
    fi.m_salbrut          AS indemnite_brute,
    fi.m_netpay           AS indemnite_nette,
    fi.m_netord           AS indemnite_net_ordinaire,
    fi.m_salimp           AS indemnite_imposable,
    fi.m_salnimp          AS indemnite_non_imposable,
    fi.m_cpe              AS cotisation_patronale,
    fi.m_retrait          AS retenue,
    fi.dq_grade_matched,
    fi.dq_org_matched,
    fi.dq_has_issues
FROM dw.fact_indem fi
WHERE fi.employee_sk <> 0
  AND fi.time_sk     <> 0;

COMMENT ON VIEW dw.vw_dw2_fact_indem IS
    'DW2 — Indemnity fact filtered to paie-matched employees only (intersection).';


CREATE OR REPLACE VIEW dw.vw_dw2_dim_employee AS
SELECT
    employee_sk,
    employee_id,
    COALESCE(last_name,  'INCONNU') AS last_name,
    COALESCE(first_name, 'INCONNU') AS first_name,
    gender,
    birth_date,
    hire_date,
    appointment_date
FROM dw.dim_employee
WHERE NOT is_unknown;

CREATE OR REPLACE VIEW dw.vw_dw2_dim_temps AS
SELECT
    time_sk,
    year_num         AS annee,
    month_num        AS mois,
    year_month,
    quarter_num      AS trimestre,
    semester_num     AS semestre,
    month_start_date
FROM dw.dim_temps
WHERE NOT is_unknown;

CREATE OR REPLACE VIEW dw.vw_dw2_dim_grade AS
SELECT
    grade_sk,
    grade_code,
    grade_label_fr,
    grade_label_ar,
    category         AS categorie
FROM dw.dim_grade
WHERE NOT is_unknown;

CREATE OR REPLACE VIEW dw.vw_dw2_dim_nature AS
SELECT
    nature_sk,
    nature_code,
    nature_label_fr,
    nature_label_ar
FROM dw.dim_nature
WHERE NOT is_unknown;

CREATE OR REPLACE VIEW dw.vw_dw2_dim_organisme AS
SELECT
    organisme_sk,
    codetab          AS ministry_code,
    dire             AS direction_code,
    liborgl          AS ministere_fr,
    liborga          AS ministere_ar
FROM dw.dim_organisme
WHERE NOT is_unknown;

CREATE OR REPLACE VIEW dw.vw_dw2_dim_region AS
SELECT
    region_sk,
    coddep           AS region_code,
    lib_reg          AS region_fr,
    lib_rega         AS region_ar,
    codreg           AS code_region
FROM dw.dim_region
WHERE NOT is_unknown;

CREATE OR REPLACE VIEW dw.vw_dw2_dim_indemnite AS
SELECT
    indemnite_sk,
    indemnite_code,
    indemnite_label_fr      AS indemnite_fr,
    indemnite_label_fr_long AS indemnite_fr_long,
    indemnite_label_ar      AS indemnite_ar,
    nature_flag,
    is_taxable,
    is_cnr,
    zone
FROM dw.dim_indemnite
WHERE NOT is_unknown;
