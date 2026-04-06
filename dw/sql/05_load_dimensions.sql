-- ============================================================
-- INSAF Payroll Intelligence Platform
-- Script 05 — Load shared dimensions from clean JSONL files
--
-- Execution order:
--   1. Run Python ETL pipelines first to generate clean JSONL.
--   2. Load staging via \copy (see comments below).
--   3. Run this script to populate dw.dim_* tables.
--
-- \copy commands (run in psql — adjust paths as needed):
--   \copy stg.dim_employee_raw   FROM '/path/dim_employee.jsonl'
--   \copy stg.dim_grade_raw      FROM '/path/dim_grade.jsonl'
--   \copy stg.dim_nature_raw     FROM '/path/dim_nature.jsonl'
--   \copy stg.dim_organisme_raw  FROM '/path/dim_organisme.jsonl'
--   \copy stg.dim_region_raw     FROM '/path/dim_region.jsonl'
--   \copy stg.dim_time_raw       FROM '/path/dim_time.jsonl'
--   \copy stg.dim_indemnite_raw  FROM '/path/dim_indemnite.jsonl'
-- ============================================================

-- ── Staging tables ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stg.dim_employee_raw  (raw JSONB);
CREATE TABLE IF NOT EXISTS stg.dim_grade_raw     (raw JSONB);
CREATE TABLE IF NOT EXISTS stg.dim_nature_raw    (raw JSONB);
CREATE TABLE IF NOT EXISTS stg.dim_organisme_raw (raw JSONB);
CREATE TABLE IF NOT EXISTS stg.dim_region_raw    (raw JSONB);
CREATE TABLE IF NOT EXISTS stg.dim_time_raw      (raw JSONB);
CREATE TABLE IF NOT EXISTS stg.dim_indemnite_raw (raw JSONB);

-- Truncate staging before each load to avoid duplicates
TRUNCATE stg.dim_employee_raw, stg.dim_grade_raw, stg.dim_nature_raw,
         stg.dim_organisme_raw, stg.dim_region_raw, stg.dim_time_raw,
         stg.dim_indemnite_raw;

-- ════════════════════════════════════════════════════════════════
-- After running \copy above, execute the inserts below.
-- ════════════════════════════════════════════════════════════════

-- ── dim_employee ──────────────────────────────────────────────────────────────
INSERT INTO dw.dim_employee
    (employee_id, last_name, first_name, gender, birth_date, hire_date)
SELECT DISTINCT
    NULLIF(TRIM(raw->>'employee_id'), ''),
    NULLIF(TRIM(raw->>'last_name'),   ''),
    NULLIF(TRIM(raw->>'first_name'),  ''),
    CASE WHEN raw->>'gender' IN ('1','2') THEN (raw->>'gender')::SMALLINT END,
    CASE WHEN raw->>'birth_date' ~ '^\d{4}-\d{2}-\d{2}$'
         THEN (raw->>'birth_date')::DATE END,
    CASE WHEN raw->>'hire_date'  ~ '^\d{4}-\d{2}-\d{2}$'
         THEN (raw->>'hire_date')::DATE  END
FROM stg.dim_employee_raw
WHERE NULLIF(TRIM(raw->>'employee_id'), '') IS NOT NULL
  AND NULLIF(TRIM(raw->>'employee_id'), '') <> 'UNKNOWN'
ON CONFLICT (employee_id) DO UPDATE SET
    last_name  = EXCLUDED.last_name,
    first_name = EXCLUDED.first_name,
    gender     = EXCLUDED.gender,
    birth_date = EXCLUDED.birth_date,
    hire_date  = EXCLUDED.hire_date,
    dw_load_ts = NOW();

-- ── dim_grade ─────────────────────────────────────────────────────────────────
INSERT INTO dw.dim_grade
    (grade_code, grade_label_fr, grade_label_ar, category, class_grade, retire_age)
SELECT DISTINCT
    UPPER(TRIM(raw->>'grade_code')),
    NULLIF(TRIM(raw->>'grade_label_fr'), ''),
    NULLIF(TRIM(raw->>'grade_label_ar'), ''),
    NULLIF(TRIM(raw->>'category'),       ''),
    NULLIF(TRIM(raw->>'class_grade'),    ''),
    CASE WHEN raw->>'retire_age' ~ '^\d+$'
         THEN (raw->>'retire_age')::SMALLINT END
FROM stg.dim_grade_raw
WHERE NULLIF(TRIM(raw->>'grade_code'), '') IS NOT NULL
  AND UPPER(TRIM(raw->>'grade_code')) <> '???'
ON CONFLICT (grade_code) DO UPDATE SET
    grade_label_fr = EXCLUDED.grade_label_fr,
    grade_label_ar = EXCLUDED.grade_label_ar,
    dw_load_ts     = NOW();

-- ── dim_nature ────────────────────────────────────────────────────────────────
INSERT INTO dw.dim_nature
    (nature_code, nature_type, nature_label_fr, nature_label_ar)
SELECT DISTINCT
    UPPER(TRIM(raw->>'nature_code')),
    NULLIF(TRIM(raw->>'nature_type'),     ''),
    NULLIF(TRIM(raw->>'nature_label_fr'), ''),
    NULLIF(TRIM(raw->>'nature_label_ar'), '')
FROM stg.dim_nature_raw
WHERE NULLIF(TRIM(raw->>'nature_code'), '') IS NOT NULL
  AND UPPER(TRIM(raw->>'nature_code')) <> '?'
ON CONFLICT (nature_code) DO UPDATE SET
    nature_label_fr = EXCLUDED.nature_label_fr,
    nature_label_ar = EXCLUDED.nature_label_ar,
    dw_load_ts      = NOW();

-- ── dim_organisme ─────────────────────────────────────────────────────────────
INSERT INTO dw.dim_organisme
    (codetab, cab, sg, dg, dire, sdir, serv, unite,
     liborgl, liborga, codgouv, deleg, typstruct)
SELECT DISTINCT
    NULLIF(TRIM(raw->>'codetab'), ''),
    NULLIF(TRIM(raw->>'cab'),     ''),
    NULLIF(TRIM(raw->>'sg'),      ''),
    NULLIF(TRIM(raw->>'dg'),      ''),
    NULLIF(TRIM(raw->>'dire'),    ''),
    NULLIF(TRIM(raw->>'sdir'),    ''),
    NULLIF(TRIM(raw->>'serv'),    ''),
    NULLIF(TRIM(raw->>'unite'),   ''),
    NULLIF(TRIM(raw->>'liborgl'), ''),
    NULLIF(TRIM(raw->>'liborga'), ''),
    NULLIF(TRIM(raw->>'codgouv'), ''),
    NULLIF(TRIM(raw->>'deleg'),   ''),
    NULLIF(TRIM(raw->>'typstruct'), '')
FROM stg.dim_organisme_raw
WHERE NULLIF(TRIM(raw->>'codetab'), '') IS NOT NULL
  AND NULLIF(TRIM(raw->>'dire'),    '') IS NOT NULL
  AND TRIM(raw->>'codetab') <> '???'
ON CONFLICT (codetab, dire) DO UPDATE SET
    liborgl    = EXCLUDED.liborgl,
    liborga    = EXCLUDED.liborga,
    dw_load_ts = NOW();

-- ── dim_region ────────────────────────────────────────────────────────────────
INSERT INTO dw.dim_region
    (coddep, codreg, lib_reg, lib_rega, code_dept, code_region)
SELECT DISTINCT
    NULLIF(TRIM(raw->>'coddep'),      ''),
    NULLIF(TRIM(raw->>'codreg'),      ''),
    NULLIF(TRIM(raw->>'lib_reg'),     ''),
    NULLIF(TRIM(raw->>'lib_rega'),    ''),
    NULLIF(TRIM(raw->>'code_dept'),   ''),
    NULLIF(TRIM(raw->>'code_region'), '')
FROM stg.dim_region_raw
WHERE NULLIF(TRIM(raw->>'coddep'), '') IS NOT NULL
  AND NULLIF(TRIM(raw->>'codreg'), '') IS NOT NULL
  AND TRIM(raw->>'coddep') <> '???'
ON CONFLICT (coddep, codreg) DO UPDATE SET
    lib_reg    = EXCLUDED.lib_reg,
    lib_rega   = EXCLUDED.lib_rega,
    dw_load_ts = NOW();

-- ── dim_temps ─────────────────────────────────────────────────────────────────
INSERT INTO dw.dim_temps
    (year_num, month_num, year_month, month_start_date)
SELECT DISTINCT
    (raw->>'year_num')::SMALLINT,
    (raw->>'month_num')::SMALLINT,
    TRIM(raw->>'year_month'),
    (raw->>'month_start_date')::DATE
FROM stg.dim_time_raw
WHERE raw ? 'year_num' AND raw ? 'month_num'
  AND (raw->>'year_num')::INT BETWEEN 2000 AND 2099
  AND (raw->>'month_num')::INT BETWEEN 1 AND 12
ON CONFLICT (year_num, month_num) DO NOTHING;

-- ── dim_indemnite ─────────────────────────────────────────────────────────────
INSERT INTO dw.dim_indemnite
    (indemnite_code, indemnite_label_fr, indemnite_label_fr_long,
     indemnite_label_ar, nature_flag, is_taxable, is_cnr,
     zone, arg1, arg2, date_entry, insurance_code)
SELECT DISTINCT
    TRIM(raw->>'indemnite_code'),
    NULLIF(TRIM(raw->>'indemnite_label_fr'),      ''),
    NULLIF(TRIM(raw->>'indemnite_label_fr_long'), ''),
    NULLIF(TRIM(raw->>'indemnite_label_ar'),      ''),
    NULLIF(TRIM(raw->>'nature_flag'),             ''),
    (raw->>'is_taxable')::BOOLEAN,
    (raw->>'is_cnr')::BOOLEAN,
    NULLIF(TRIM(raw->>'zone'),           ''),
    CASE WHEN raw->>'arg1' ~ '^-?\d+(\.\d+)?$' THEN (raw->>'arg1')::NUMERIC END,
    CASE WHEN raw->>'arg2' ~ '^-?\d+(\.\d+)?$' THEN (raw->>'arg2')::NUMERIC END,
    CASE WHEN raw->>'date_entry' ~ '^\d{4}-\d{2}-\d{2}$'
         THEN (raw->>'date_entry')::DATE END,
    NULLIF(TRIM(raw->>'insurance_code'), '')
FROM stg.dim_indemnite_raw
WHERE NULLIF(TRIM(raw->>'indemnite_code'), '') IS NOT NULL
  AND TRIM(raw->>'indemnite_code') <> '????'
ON CONFLICT (indemnite_code) DO UPDATE SET
    indemnite_label_fr      = EXCLUDED.indemnite_label_fr,
    indemnite_label_fr_long = EXCLUDED.indemnite_label_fr_long,
    indemnite_label_ar      = EXCLUDED.indemnite_label_ar,
    dw_load_ts              = NOW();

-- ── Reset sequences after bulk insert ────────────────────────────────────────
SELECT setval(pg_get_serial_sequence('dw.dim_employee',  'employee_sk'),
              GREATEST((SELECT MAX(employee_sk)  FROM dw.dim_employee),  1));
SELECT setval(pg_get_serial_sequence('dw.dim_grade',     'grade_sk'),
              GREATEST((SELECT MAX(grade_sk)     FROM dw.dim_grade),     1));
SELECT setval(pg_get_serial_sequence('dw.dim_nature',    'nature_sk'),
              GREATEST((SELECT MAX(nature_sk)    FROM dw.dim_nature),    1));
SELECT setval(pg_get_serial_sequence('dw.dim_organisme', 'organisme_sk'),
              GREATEST((SELECT MAX(organisme_sk) FROM dw.dim_organisme), 1));
SELECT setval(pg_get_serial_sequence('dw.dim_region',    'region_sk'),
              GREATEST((SELECT MAX(region_sk)    FROM dw.dim_region),    1));
SELECT setval(pg_get_serial_sequence('dw.dim_temps',     'time_sk'),
              GREATEST((SELECT MAX(time_sk)      FROM dw.dim_temps),     1));
SELECT setval(pg_get_serial_sequence('dw.dim_indemnite', 'indemnite_sk'),
              GREATEST((SELECT MAX(indemnite_sk) FROM dw.dim_indemnite), 1));
