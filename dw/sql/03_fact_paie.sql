-- ============================================================
-- INSAF Payroll Intelligence Platform
-- Script 03 — DW1: fact_paie  (payroll, pa_type = '1')
--
-- Star schema:
--   fact_paie → dim_employee, dim_temps, dim_grade,
--               dim_nature, dim_organisme, dim_region
--
-- Grain: one row per (employee × time × pa_type)
--
-- Execution: run after 02_shared_dimensions.sql
-- ============================================================

SET search_path = dw, public;

CREATE TABLE IF NOT EXISTS dw.fact_paie (
    -- ── Grain / natural key ─────────────────────────────────────────────────
    employee_sk     BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_employee(employee_sk),
    time_sk         BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_temps(time_sk),
    pa_type         CHAR(1)     NOT NULL DEFAULT '1',

    -- ── Dimension foreign keys ───────────────────────────────────────────────
    grade_sk        BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_grade(grade_sk),
    nature_sk       BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_nature(nature_sk),
    organisme_sk    BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_organisme(organisme_sk),
    region_sk       BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_region(region_sk),

    -- ── Degenerate dimensions (low-cardinality, kept in fact) ────────────────
    pa_eche         SMALLINT,               -- salary scale echelon
    pa_sitfam       CHAR(1),                -- marital status
    pa_loca_raw     TEXT,                   -- raw locality code (no crosswalk yet)

    -- ── Salary measures (NULL = field absent in source, 0 = genuine zero) ────
    m_salimp        NUMERIC(15,3),          -- taxable salary
    m_salnimp       NUMERIC(15,3),          -- non-taxable salary
    m_salbrut       NUMERIC(15,3),          -- gross salary
    m_brutcnr       NUMERIC(15,3),          -- gross CNR basis
    m_netord        NUMERIC(15,3),          -- net ordinary
    m_netpay        NUMERIC(15,3),          -- ★ NET PAY — primary KPI

    -- ── Deduction measures ───────────────────────────────────────────────────
    m_cpe           NUMERIC(15,3),          -- employer social contribution
    m_retrait       NUMERIC(15,3),          -- pension deduction
    m_cps           NUMERIC(15,3),          -- social security
    m_capdeces      NUMERIC(15,3),          -- death benefit

    -- ── Allowance measures ───────────────────────────────────────────────────
    m_avkm          NUMERIC(15,3),          -- km allowance
    m_avlog         NUMERIC(15,3),          -- housing allowance

    -- ── Report / subsidy measures ────────────────────────────────────────────
    m_rapimp        NUMERIC(15,3),
    m_rapni         NUMERIC(15,3),
    m_sub           NUMERIC(15,3),
    m_sps           NUMERIC(15,3),
    m_spl           NUMERIC(15,3),
    m_rapsalb       NUMERIC(15,3),

    -- ── Data quality metadata ────────────────────────────────────────────────
    dq_grade_matched      BOOLEAN NOT NULL DEFAULT FALSE,
    dq_nature_matched     BOOLEAN NOT NULL DEFAULT FALSE,
    dq_org_matched        BOOLEAN NOT NULL DEFAULT FALSE,
    dq_region_matched     BOOLEAN NOT NULL DEFAULT FALSE,
    dq_has_issues         BOOLEAN NOT NULL DEFAULT FALSE,
    dq_issue_count        SMALLINT NOT NULL DEFAULT 0,

    -- ── Audit ────────────────────────────────────────────────────────────────
    run_id          TEXT        NOT NULL,
    source_file     TEXT        NOT NULL,
    load_ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- ── Grain constraint ─────────────────────────────────────────────────────
    CONSTRAINT pk_fact_paie PRIMARY KEY (employee_sk, time_sk, pa_type)
);

-- Analytic indexes
CREATE INDEX IF NOT EXISTS idx_fp_time     ON dw.fact_paie (time_sk);
CREATE INDEX IF NOT EXISTS idx_fp_grade    ON dw.fact_paie (grade_sk);
CREATE INDEX IF NOT EXISTS idx_fp_nature   ON dw.fact_paie (nature_sk);
CREATE INDEX IF NOT EXISTS idx_fp_org      ON dw.fact_paie (organisme_sk);
CREATE INDEX IF NOT EXISTS idx_fp_region   ON dw.fact_paie (region_sk);
CREATE INDEX IF NOT EXISTS idx_fp_netpay   ON dw.fact_paie (m_netpay)
    WHERE m_netpay IS NOT NULL;

COMMENT ON TABLE dw.fact_paie IS
    'DW1 — payroll records (pa_type=1). Grain: employee × month × pa_type.
     Measures are NULL when absent in source, 0 only when source value is 0.';
COMMENT ON COLUMN dw.fact_paie.m_netpay    IS 'Final net pay — primary BI KPI.';
COMMENT ON COLUMN dw.fact_paie.pa_loca_raw IS 'Raw locality code from source. No crosswalk available; preserved for future use.';
