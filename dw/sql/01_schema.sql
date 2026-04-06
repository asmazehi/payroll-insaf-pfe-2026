-- ============================================================
-- INSAF Payroll Intelligence Platform
-- Script 01 — Schema bootstrap + audit log
-- Run this ONCE before anything else.
-- ============================================================

-- Schemas
CREATE SCHEMA IF NOT EXISTS dw;       -- all DW tables (shared dims + 2 facts)
CREATE SCHEMA IF NOT EXISTS stg;      -- staging tables (temporary JSONB landing)
CREATE SCHEMA IF NOT EXISTS audit;    -- pipeline run logs

-- Audit / run log — every ETL run writes one row here
CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    run_id          TEXT        PRIMARY KEY,
    pipeline        TEXT        NOT NULL,   -- 'DW1_paie' | 'DW2_indem'
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    source_file     TEXT,
    rows_written    INTEGER,
    qg_status       TEXT,                   -- 'PASS' | 'FAIL' | 'PASS_WITH_WARNINGS'
    qg_errors       TEXT[],
    qg_warnings     TEXT[],
    created_ts      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE audit.pipeline_run IS
    'One row per ETL pipeline execution. Used for lineage and debugging.';
