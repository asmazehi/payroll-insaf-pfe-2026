-- Users table
CREATE TABLE IF NOT EXISTS public.users (
    id             BIGSERIAL    PRIMARY KEY,
    username       VARCHAR(50)  UNIQUE NOT NULL,
    email          VARCHAR(100) UNIQUE NOT NULL,
    password       VARCHAR(255) NOT NULL,
    role           VARCHAR(20)  NOT NULL DEFAULT 'ROLE_USER',
    ministry_code  VARCHAR(10)  DEFAULT NULL,
    enabled        BOOLEAN      NOT NULL DEFAULT TRUE
);

ALTER TABLE public.users ADD COLUMN IF NOT EXISTS ministry_code     VARCHAR(10)  DEFAULT NULL;
ALTER TABLE public.anomaly_reviews ADD COLUMN IF NOT EXISTS dismissed_at TIMESTAMPTZ DEFAULT NULL;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS phone            VARCHAR(30)  DEFAULT NULL;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS profession       VARCHAR(100) DEFAULT NULL;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS profile_photo    TEXT         DEFAULT NULL;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS password_changed BOOLEAN      NOT NULL DEFAULT FALSE;

-- ETL job tracking
CREATE TABLE IF NOT EXISTS public.etl_jobs (
    id           BIGSERIAL    PRIMARY KEY,
    run_id       VARCHAR(20)  NOT NULL UNIQUE,
    file_name    VARCHAR(255) NOT NULL,
    file_type    VARCHAR(10)  NOT NULL,
    status       VARCHAR(20)  NOT NULL DEFAULT 'RUNNING',
    started_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at  TIMESTAMPTZ,
    rows_written INTEGER,
    qg_status    VARCHAR(40),
    error_detail TEXT,
    uploaded_by  VARCHAR(50)
);

-- Establishment dimension (ministry-level, joins to dim_organisme via codetab)
CREATE TABLE IF NOT EXISTS dw.dim_etablissement (
    etablissement_sk  BIGSERIAL    PRIMARY KEY,
    codetab           CHAR(3)      NOT NULL UNIQUE,
    natorg            VARCHAR(5),
    libcetabl         TEXT,
    libcetaba         TEXT,
    libletabl         TEXT,
    libletaba         TEXT,
    sigle_etab        VARCHAR(20),
    typgest           VARCHAR(5),
    codgest           VARCHAR(5),
    adretabl          TEXT,
    adretaba          TEXT,
    teletab           VARCHAR(30),
    resp_etabl        TEXT,
    resp_etaba        TEXT,
    etat_etab         VARCHAR(5),
    code_resp         VARCHAR(5),
    stutel            VARCHAR(20),
    codtutel          VARCHAR(10),
    codchap           VARCHAR(10),
    codsec            VARCHAR(10),
    subv              VARCHAR(20),
    dw_load_ts        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Raw codetab on fact_paie for direct join to dim_etablissement
ALTER TABLE dw.fact_paie ADD COLUMN IF NOT EXISTS codetab CHAR(3);

-- Support tickets
CREATE TABLE IF NOT EXISTS public.tickets (
    id           BIGSERIAL    PRIMARY KEY,
    title        VARCHAR(200) NOT NULL,
    description  TEXT,
    status       VARCHAR(20)  NOT NULL DEFAULT 'OPEN',
    ministry_code VARCHAR(10),
    created_by   VARCHAR(50)  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved_at  TIMESTAMPTZ
);
ALTER TABLE public.tickets ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;

-- Per-ministry aggregated MV — used by ministry-scoped dashboard queries (sub-ms vs 36s on raw table)
-- REFRESH MATERIALIZED VIEW dw.mv_ministry_details; — run after each ETL load
CREATE MATERIALIZED VIEW IF NOT EXISTS dw.mv_ministry_details AS
SELECT fp.codetab, dt.year_num, dt.month_num, dt.month_start_date,
    COUNT(*)                        AS record_count,
    COUNT(DISTINCT fp.employee_sk)  AS employee_count,
    SUM(fp.m_netpay)                AS total_netpay,
    SUM(fp.m_salbrut)               AS total_grosspay,
    AVG(fp.m_netpay)                AS avg_netpay
FROM dw.fact_paie fp
JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
WHERE fp.employee_sk <> 0 AND dt.year_num > 0 AND fp.codetab IS NOT NULL
GROUP BY fp.codetab, dt.year_num, dt.month_num, dt.month_start_date;

CREATE INDEX IF NOT EXISTS idx_mv_ministry_codetab  ON dw.mv_ministry_details (codetab);
CREATE INDEX IF NOT EXISTS idx_mv_ministry_year     ON dw.mv_ministry_details (codetab, year_num);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_ministry_details_uq    ON dw.mv_ministry_details  (codetab, year_num, month_num);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_payroll_by_month_uq    ON dw.mv_payroll_by_month  (year_num, month_num);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_grade_distribution_uq  ON dw.mv_grade_distribution (grade_code);
CREATE INDEX IF NOT EXISTS idx_fact_paie_codetab    ON dw.fact_paie (codetab);
CREATE INDEX IF NOT EXISTS idx_fact_paie_grade_tab  ON dw.fact_paie (codetab, grade_sk) WHERE m_netpay IS NOT NULL;

-- Ministry hierarchy view: maps each establishment codetab → its parent ministry codetab.
-- Ministries (natorg='1') self-reference; sub-establishments link via codtutel.
-- Sport federations (natorg='8') map to W00 (Ministry of Youth and Sports) when
-- codtutel is not explicitly set.
-- Used by dashboard and anomaly filters to include all sub-establishments when
-- a ministry-level user is querying data.
CREATE OR REPLACE VIEW dw.v_ministry_codetabs AS
    -- Every establishment references itself (fallback — always returns at least 1 row per codetab)
    SELECT codetab AS sub_codetab, codetab AS ministry_codetab
    FROM dw.dim_etablissement

    UNION

    -- Sub-establishments with an explicit parent ministry via codtutel
    -- (populated by the ETL loader using keyword matching + explicit reference data)
    SELECT codetab AS sub_codetab, codtutel AS ministry_codetab
    FROM dw.dim_etablissement
    WHERE codtutel IS NOT NULL AND codtutel <> codetab;

-- Default admin (password: admin123)
INSERT INTO public.users (username, email, password, role)
VALUES ('admin', 'admin@insaf.tn',
        '$2b$10$h5umgFwMph3tsNUnuO.iSec.9udxqA.cBEY3HVGztMEeAWo60kimW',
        'ROLE_ADMIN')
ON CONFLICT (username) DO NOTHING;
