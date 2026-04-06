-- ============================================================
-- INSAF Payroll Intelligence Platform
-- Script 04 — DW2: fact_indem  (indemnities, pa_type = '3')
--
-- Star schema:
--   fact_indem → dim_employee, dim_temps, dim_grade,
--                dim_nature,   dim_organisme, dim_region,
--                dim_indemnite  (DW2-specific dimension)
--
-- Grain: one row per (employee × time × pa_type)
--
-- Note: dim_indemnite is the extra dimension that
-- distinguishes DW2 from DW1.
--
-- Execution: run after 02_shared_dimensions.sql
-- ============================================================

SET search_path = dw, public;

CREATE TABLE IF NOT EXISTS dw.fact_indem (
    -- ── Grain / natural key ─────────────────────────────────────────────────
    employee_sk     BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_employee(employee_sk),
    time_sk         BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_temps(time_sk),
    pa_type         CHAR(1)     NOT NULL DEFAULT '3',

    -- ── Dimension foreign keys (shared with DW1) ─────────────────────────────
    grade_sk        BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_grade(grade_sk),
    nature_sk       BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_nature(nature_sk),
    organisme_sk    BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_organisme(organisme_sk),
    region_sk       BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_region(region_sk),

    -- ── DW2-specific dimension ───────────────────────────────────────────────
    -- Note: ind2015.json does not carry an explicit indemnity code per row.
    -- dim_indemnite is populated from indem_def.json for BI reference.
    -- indemnite_sk kept here for future linking when the code column is exposed.
    indemnite_sk    BIGINT      NOT NULL DEFAULT 0
                                REFERENCES dw.dim_indemnite(indemnite_sk),

    -- ── Degenerate dimensions ────────────────────────────────────────────────
    pa_eche         SMALLINT,
    pa_sitfam       CHAR(1),
    pa_loca_raw     TEXT,

    -- ── Measures — same schema as fact_paie (same source columns) ────────────
    m_salimp        NUMERIC(15,3),
    m_salnimp       NUMERIC(15,3),
    m_salbrut       NUMERIC(15,3),
    m_brutcnr       NUMERIC(15,3),
    m_netord        NUMERIC(15,3),
    m_netpay        NUMERIC(15,3),          -- ★ net indemnity pay
    m_cpe           NUMERIC(15,3),
    m_retrait       NUMERIC(15,3),
    m_cps           NUMERIC(15,3),
    m_capdeces      NUMERIC(15,3),
    m_avkm          NUMERIC(15,3),
    m_avlog         NUMERIC(15,3),
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
    CONSTRAINT pk_fact_indem PRIMARY KEY (employee_sk, time_sk, pa_type)
);

-- Analytic indexes
CREATE INDEX IF NOT EXISTS idx_fi_time       ON dw.fact_indem (time_sk);
CREATE INDEX IF NOT EXISTS idx_fi_grade      ON dw.fact_indem (grade_sk);
CREATE INDEX IF NOT EXISTS idx_fi_nature     ON dw.fact_indem (nature_sk);
CREATE INDEX IF NOT EXISTS idx_fi_org        ON dw.fact_indem (organisme_sk);
CREATE INDEX IF NOT EXISTS idx_fi_region     ON dw.fact_indem (region_sk);
CREATE INDEX IF NOT EXISTS idx_fi_indemnite  ON dw.fact_indem (indemnite_sk);
CREATE INDEX IF NOT EXISTS idx_fi_netpay     ON dw.fact_indem (m_netpay)
    WHERE m_netpay IS NOT NULL;

COMMENT ON TABLE dw.fact_indem IS
    'DW2 — indemnity records (pa_type=3). Grain: employee × month × pa_type.
     Shares all 6 base dimensions with fact_paie; adds dim_indemnite.';
COMMENT ON COLUMN dw.fact_indem.m_netpay IS 'Net indemnity pay — primary BI KPI for DW2.';
