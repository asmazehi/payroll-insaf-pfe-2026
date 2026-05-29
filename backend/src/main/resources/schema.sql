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

ALTER TABLE public.users ADD COLUMN IF NOT EXISTS ministry_code  VARCHAR(10)  DEFAULT NULL;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS phone         VARCHAR(30)  DEFAULT NULL;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS profession    VARCHAR(100) DEFAULT NULL;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS profile_photo TEXT         DEFAULT NULL;

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

-- Default admin (password: admin123)
INSERT INTO public.users (username, email, password, role)
VALUES ('admin', 'admin@insaf.tn',
        '$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lE9lBOsl7iKTVKIUi',
        'ROLE_ADMIN')
ON CONFLICT (username) DO NOTHING;
