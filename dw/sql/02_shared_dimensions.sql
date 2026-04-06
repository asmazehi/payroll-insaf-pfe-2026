-- ============================================================
-- INSAF Payroll Intelligence Platform
-- Script 02 — Shared dimension tables
--
-- These 7 dimensions are SHARED between DW1 (fact_paie)
-- and DW2 (fact_indem).  Both fact tables reference the
-- same surrogate keys.
--
-- Unknown member convention
-- ─────────────────────────
--   Every dimension has a row with sk = 0 and is_unknown = TRUE.
--   Fact rows that cannot be matched to a dimension get sk = 0.
--   This is explicit and queryable — NOT a NULL foreign key.
--
-- Execution: run after 01_schema.sql
-- ============================================================

SET search_path = dw, public;

-- ── dim_employee ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_employee (
    employee_sk  BIGSERIAL    PRIMARY KEY,
    employee_id  TEXT         NOT NULL,          -- pa_mat (source business key)
    last_name    TEXT,
    first_name   TEXT,
    gender       SMALLINT     CHECK (gender IN (1, 2)),   -- 1=M  2=F
    birth_date   DATE,                           -- NULL = unknown / absent
    hire_date         DATE,
    appointment_date  DATE,
    is_unknown   BOOLEAN      NOT NULL DEFAULT FALSE,
    dw_load_ts   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_employee_id UNIQUE (employee_id)
);

INSERT INTO dw.dim_employee
    (employee_sk, employee_id, last_name, first_name, is_unknown)
VALUES (0, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', TRUE)
ON CONFLICT (employee_id) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('dw.dim_employee','employee_sk'),
    GREATEST((SELECT MAX(employee_sk) FROM dw.dim_employee), 0)
);


-- ── dim_grade ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_grade (
    grade_sk       BIGSERIAL   PRIMARY KEY,
    grade_code     CHAR(3)     NOT NULL,          -- CODGRD
    grade_label_fr TEXT,
    grade_label_ar TEXT,                          -- Arabic label (encoding fixed)
    category       TEXT,                          -- CAT (A/B/C/…)
    class_grade    TEXT,
    retire_age     SMALLINT,
    is_unknown     BOOLEAN     NOT NULL DEFAULT FALSE,
    dw_load_ts     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_grade_code UNIQUE (grade_code)
);

INSERT INTO dw.dim_grade (grade_sk, grade_code, is_unknown)
VALUES (0, '???', TRUE)
ON CONFLICT (grade_code) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('dw.dim_grade','grade_sk'),
    GREATEST((SELECT MAX(grade_sk) FROM dw.dim_grade), 0)
);


-- ── dim_nature ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_nature (
    nature_sk        BIGSERIAL   PRIMARY KEY,
    nature_code      CHAR(1)     NOT NULL,        -- CODNAT
    nature_type      CHAR(1),                     -- TYPNAT (1=civil 2=worker 3=contract)
    nature_label_fr  TEXT,
    nature_label_ar  TEXT,                        -- Arabic label (encoding fixed)
    is_unknown       BOOLEAN     NOT NULL DEFAULT FALSE,
    dw_load_ts       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_nature_code UNIQUE (nature_code)
);

INSERT INTO dw.dim_nature (nature_sk, nature_code, is_unknown)
VALUES (0, '?', TRUE)
ON CONFLICT (nature_code) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('dw.dim_nature','nature_sk'),
    GREATEST((SELECT MAX(nature_sk) FROM dw.dim_nature), 0)
);


-- ── dim_organisme ─────────────────────────────────────────────────────────────
-- Business key: (codetab, dire) — minimum viable composite key
-- We never fill blanks with '000'; those orgs map to sk=0 (Unknown).
CREATE TABLE IF NOT EXISTS dw.dim_organisme (
    organisme_sk  BIGSERIAL   PRIMARY KEY,
    codetab       CHAR(3),
    cab           CHAR(3),
    sg            CHAR(3),
    dg            CHAR(3),
    dire          CHAR(3),
    sdir          CHAR(3),
    serv          CHAR(3),
    unite         CHAR(3),
    liborgl       TEXT,                           -- French label
    liborga       TEXT,                           -- Arabic label (encoding fixed)
    codgouv       CHAR(1),
    deleg         CHAR(1),
    typstruct     TEXT,
    is_unknown    BOOLEAN     NOT NULL DEFAULT FALSE,
    dw_load_ts    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_organisme UNIQUE (codetab, dire)
);

INSERT INTO dw.dim_organisme (organisme_sk, codetab, dire, is_unknown)
VALUES (0, '???', '???', TRUE)
ON CONFLICT (codetab, dire) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('dw.dim_organisme','organisme_sk'),
    GREATEST((SELECT MAX(organisme_sk) FROM dw.dim_organisme), 0)
);


-- ── dim_region ────────────────────────────────────────────────────────────────
-- Join key: coddep (= pa_codmin).
-- pa_loca has no crosswalk — rows with no match get sk=0.
CREATE TABLE IF NOT EXISTS dw.dim_region (
    region_sk    BIGSERIAL   PRIMARY KEY,
    coddep       CHAR(3),                         -- ministry/department code
    codreg       TEXT,
    lib_reg      TEXT,                            -- system abbreviation
    lib_rega     TEXT,                            -- Arabic label (encoding fixed)
    code_dept    TEXT,
    code_region  TEXT,
    is_unknown   BOOLEAN     NOT NULL DEFAULT FALSE,
    dw_load_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_region UNIQUE (coddep, codreg)
);

INSERT INTO dw.dim_region (region_sk, coddep, codreg, is_unknown)
VALUES (0, '???', '???', TRUE)
ON CONFLICT (coddep, codreg) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('dw.dim_region','region_sk'),
    GREATEST((SELECT MAX(region_sk) FROM dw.dim_region), 0)
);


-- ── dim_temps ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_temps (
    time_sk          BIGSERIAL   PRIMARY KEY,
    year_num         SMALLINT    NOT NULL,
    month_num        SMALLINT    NOT NULL CHECK (month_num BETWEEN 1 AND 12),
    year_month       CHAR(7)     NOT NULL,        -- 'YYYY-MM'
    month_start_date DATE        NOT NULL,
    quarter_num      SMALLINT    GENERATED ALWAYS AS
                         (CEIL(month_num::numeric / 3)::SMALLINT) STORED,
    semester_num     SMALLINT    GENERATED ALWAYS AS
                         (CEIL(month_num::numeric / 6)::SMALLINT) STORED,
    is_unknown       BOOLEAN     NOT NULL DEFAULT FALSE,
    dw_load_ts       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_temps UNIQUE (year_num, month_num)
);

INSERT INTO dw.dim_temps
    (time_sk, year_num, month_num, year_month, month_start_date, is_unknown)
VALUES (0, 0, 1, '0000-01', '0001-01-01', TRUE)
ON CONFLICT DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('dw.dim_temps','time_sk'),
    GREATEST((SELECT MAX(time_sk) FROM dw.dim_temps), 0)
);


-- ── dim_indemnite ─────────────────────────────────────────────────────────────
-- Reference for indemnity codes (from indem_def.json).
-- Used by DW2 (fact_indem) only — harmless to create here alongside other dims.
CREATE TABLE IF NOT EXISTS dw.dim_indemnite (
    indemnite_sk          BIGSERIAL   PRIMARY KEY,
    indemnite_code        TEXT        NOT NULL,   -- TMI_CIND  (4-char)
    indemnite_label_fr    TEXT,                   -- TMI_LIBC  (short)
    indemnite_label_fr_long TEXT,                 -- TMI_LIBL  (full)
    indemnite_label_ar    TEXT,                   -- TMI_LIBA  (Arabic, fixed)
    nature_flag           TEXT,                   -- TMI_NAT
    is_taxable            BOOLEAN,                -- TMI_IMP = '1'
    is_cnr                BOOLEAN,                -- TMI_CNR  = '1'
    zone                  TEXT,                   -- TMI_ZON
    arg1                  NUMERIC(10,4),          -- TMI_ARG1
    arg2                  NUMERIC(10,4),          -- TMI_ARG2
    date_entry            DATE,                   -- TMI_DPC
    insurance_code        TEXT,                   -- TMI_CINS
    is_unknown            BOOLEAN     NOT NULL DEFAULT FALSE,
    dw_load_ts            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_indemnite_code UNIQUE (indemnite_code)
);

INSERT INTO dw.dim_indemnite (indemnite_sk, indemnite_code, is_unknown)
VALUES (0, '????', TRUE)
ON CONFLICT (indemnite_code) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('dw.dim_indemnite','indemnite_sk'),
    GREATEST((SELECT MAX(indemnite_sk) FROM dw.dim_indemnite), 0)
);
