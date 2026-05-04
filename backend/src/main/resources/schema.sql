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

ALTER TABLE public.users ADD COLUMN IF NOT EXISTS ministry_code VARCHAR(10) DEFAULT NULL;

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

-- Default admin (password: admin123)
INSERT INTO public.users (username, email, password, role)
VALUES ('admin', 'admin@insaf.tn',
        '$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lE9lBOsl7iKTVKIUi',
        'ROLE_ADMIN')
ON CONFLICT (username) DO NOTHING;
