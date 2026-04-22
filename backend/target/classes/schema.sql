-- Run once to create the users table for Spring Security
CREATE TABLE IF NOT EXISTS public.users (
    id       BIGSERIAL PRIMARY KEY,
    username VARCHAR(50)  UNIQUE NOT NULL,
    email    VARCHAR(100) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    role     VARCHAR(20)  NOT NULL DEFAULT 'ROLE_ANALYST',
    enabled  BOOLEAN      NOT NULL DEFAULT TRUE
);

-- Default admin user (password: admin123)
INSERT INTO public.users (username, email, password, role)
VALUES ('admin', 'admin@insaf.tn',
        '$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lE9lBOsl7iKTVKIUi',
        'ROLE_ADMIN')
ON CONFLICT (username) DO NOTHING;
