-- INSAF DW - Facts
-- fact_paie grain: (pa_mat, pa_annee, pa_mois, pa_type)

CREATE TABLE IF NOT EXISTS fact_paie (
    fact_paie_sk BIGSERIAL PRIMARY KEY,

    employee_sk BIGINT NOT NULL,
    time_sk BIGINT NOT NULL,
    grade_sk BIGINT,
    nature_sk BIGINT,
    organisme_sk BIGINT,
    region_sk BIGINT,

    pa_mat VARCHAR(20) NOT NULL,
    pa_annee INTEGER NOT NULL,
    pa_mois SMALLINT NOT NULL,
    pa_type VARCHAR(2) NOT NULL,

    pa_salimp NUMERIC(14, 3),
    pa_salnimp NUMERIC(14, 3),
    pa_salbrut NUMERIC(14, 3),
    pa_sub NUMERIC(14, 3),
    pa_rapimp NUMERIC(14, 3),
    pa_rapni NUMERIC(14, 3),
    pa_rapsalb NUMERIC(14, 3),
    pa_cpe NUMERIC(14, 3),
    pa_retrait NUMERIC(14, 3),
    pa_cps NUMERIC(14, 3),
    pa_capdeces NUMERIC(14, 3),
    pa_sps NUMERIC(14, 3),
    pa_spl NUMERIC(14, 3),
    pa_netord NUMERIC(14, 3),
    pa_netpay NUMERIC(14, 3),
    pa_brutcnr NUMERIC(14, 3),

    CONSTRAINT ck_fact_paie_pa_mois CHECK (pa_mois BETWEEN 1 AND 12),
    CONSTRAINT ck_fact_paie_pa_type CHECK (pa_type IN ('1', '2', '3', '4')),
    CONSTRAINT uq_fact_paie_grain UNIQUE (pa_mat, pa_annee, pa_mois, pa_type),

    CONSTRAINT fk_fact_paie_employee FOREIGN KEY (employee_sk) REFERENCES dim_employee (employee_sk),
    CONSTRAINT fk_fact_paie_time FOREIGN KEY (time_sk) REFERENCES dim_temps (time_sk),
    CONSTRAINT fk_fact_paie_grade FOREIGN KEY (grade_sk) REFERENCES dim_grade (grade_sk),
    CONSTRAINT fk_fact_paie_nature FOREIGN KEY (nature_sk) REFERENCES dim_nature (nature_sk),
    CONSTRAINT fk_fact_paie_organisme FOREIGN KEY (organisme_sk) REFERENCES dim_organisme (organisme_sk),
    CONSTRAINT fk_fact_paie_region FOREIGN KEY (region_sk) REFERENCES dim_region (region_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_paie_employee_sk ON fact_paie (employee_sk);
CREATE INDEX IF NOT EXISTS idx_fact_paie_time_sk ON fact_paie (time_sk);
CREATE INDEX IF NOT EXISTS idx_fact_paie_grade_sk ON fact_paie (grade_sk);
CREATE INDEX IF NOT EXISTS idx_fact_paie_nature_sk ON fact_paie (nature_sk);
CREATE INDEX IF NOT EXISTS idx_fact_paie_organisme_sk ON fact_paie (organisme_sk);
CREATE INDEX IF NOT EXISTS idx_fact_paie_region_sk ON fact_paie (region_sk);

CREATE TABLE IF NOT EXISTS fact_indemnite (
    fact_indemnite_sk BIGSERIAL PRIMARY KEY,

    employee_sk BIGINT NOT NULL,
    time_sk BIGINT NOT NULL,
    grade_sk BIGINT,
    nature_sk BIGINT,
    organisme_sk BIGINT,
    region_sk BIGINT,

    pa_mat VARCHAR(20) NOT NULL,
    pa_annee INTEGER NOT NULL,
    pa_mois SMALLINT NOT NULL,
    pa_type VARCHAR(2) NOT NULL,

    pa_salimp NUMERIC(14, 3),
    pa_salnimp NUMERIC(14, 3),
    pa_salbrut NUMERIC(14, 3),
    pa_sub NUMERIC(14, 3),
    pa_rapimp NUMERIC(14, 3),
    pa_rapni NUMERIC(14, 3),
    pa_rapsalb NUMERIC(14, 3),
    pa_cpe NUMERIC(14, 3),
    pa_retrait NUMERIC(14, 3),
    pa_cps NUMERIC(14, 3),
    pa_capdeces NUMERIC(14, 3),
    pa_sps NUMERIC(14, 3),
    pa_spl NUMERIC(14, 3),
    pa_netord NUMERIC(14, 3),
    pa_netpay NUMERIC(14, 3),
    pa_brutcnr NUMERIC(14, 3),

    CONSTRAINT ck_fact_indemnite_pa_mois CHECK (pa_mois BETWEEN 1 AND 12),
    CONSTRAINT ck_fact_indemnite_pa_type CHECK (pa_type IN ('1', '2', '3', '4')),
    CONSTRAINT uq_fact_indemnite_grain UNIQUE (pa_mat, pa_annee, pa_mois, pa_type),

    CONSTRAINT fk_fact_indemnite_employee FOREIGN KEY (employee_sk) REFERENCES dim_employee (employee_sk),
    CONSTRAINT fk_fact_indemnite_time FOREIGN KEY (time_sk) REFERENCES dim_temps (time_sk),
    CONSTRAINT fk_fact_indemnite_grade FOREIGN KEY (grade_sk) REFERENCES dim_grade (grade_sk),
    CONSTRAINT fk_fact_indemnite_nature FOREIGN KEY (nature_sk) REFERENCES dim_nature (nature_sk),
    CONSTRAINT fk_fact_indemnite_organisme FOREIGN KEY (organisme_sk) REFERENCES dim_organisme (organisme_sk),
    CONSTRAINT fk_fact_indemnite_region FOREIGN KEY (region_sk) REFERENCES dim_region (region_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_indemnite_employee_sk ON fact_indemnite (employee_sk);
CREATE INDEX IF NOT EXISTS idx_fact_indemnite_time_sk ON fact_indemnite (time_sk);
CREATE INDEX IF NOT EXISTS idx_fact_indemnite_grade_sk ON fact_indemnite (grade_sk);
CREATE INDEX IF NOT EXISTS idx_fact_indemnite_nature_sk ON fact_indemnite (nature_sk);
CREATE INDEX IF NOT EXISTS idx_fact_indemnite_organisme_sk ON fact_indemnite (organisme_sk);
CREATE INDEX IF NOT EXISTS idx_fact_indemnite_region_sk ON fact_indemnite (region_sk);
