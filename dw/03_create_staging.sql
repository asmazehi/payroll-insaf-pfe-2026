-- INSAF DW - Staging layer
-- Purpose:
--   1) Land raw JSON payloads with minimal constraints.
--   2) Preserve source field names for traceability.
--   3) Prepare clean transforms into dim_* and fact_* tables.
--
-- Mapping guidance:
--   - staging.stg_schema_paie      -> fact_paie (+ dim lookups)
--   - staging.stg_schema_indemnity -> fact_indemnite (+ dim lookups)
--   - staging.stg_grade            -> dim_grade
--   - staging.stg_nature           -> dim_nature
--   - staging.stg_region           -> dim_region
--   - staging.stg_organisme        -> dim_organisme
--   - staging.stg_indem_def        -> dim_indemnite

CREATE SCHEMA IF NOT EXISTS staging;

-- =========================================================
-- Raw payroll JSON (schema-paie.json)
-- =========================================================
CREATE TABLE IF NOT EXISTS staging.stg_schema_paie (
    pa_codmin TEXT,
    pa_mat TEXT,
    pa_mois INTEGER,
    pa_annee INTEGER,
    pa_type TEXT,
    pa_noml TEXT,
    pa_prenl TEXT,
    pa_sexe TEXT,
    pa_datnais TEXT,
    pa_datent TEXT,
    pa_dg TEXT,
    pa_dire TEXT,
    pa_sdir TEXT,
    pa_serv TEXT,
    pa_article TEXT,
    pa_grd TEXT,
    pa_eche NUMERIC,
    pa_natu TEXT,
    pa_datnatu TEXT,
    pa_salimp NUMERIC,
    pa_salnimp NUMERIC,
    pa_cpe NUMERIC,
    pa_retrait NUMERIC,
    pa_cps NUMERIC,
    pa_capdeces NUMERIC,
    pa_netord NUMERIC,
    pa_netpay NUMERIC,
    pa_rapimp NUMERIC,
    pa_rapni NUMERIC,
    pa_sub NUMERIC,
    pa_sps NUMERIC,
    pa_spl NUMERIC,
    pa_rapsalb NUMERIC,
    pa_brutcnr NUMERIC,
    pa_salbrut NUMERIC,
    load_ts TIMESTAMPTZ DEFAULT now(),
    source_file TEXT
);

-- Common joins/filters before dimension/fact loading
CREATE INDEX IF NOT EXISTS idx_stg_schema_paie_pa_grd ON staging.stg_schema_paie (pa_grd);
CREATE INDEX IF NOT EXISTS idx_stg_schema_paie_pa_natu ON staging.stg_schema_paie (pa_natu);
CREATE INDEX IF NOT EXISTS idx_stg_schema_paie_pa_codmin ON staging.stg_schema_paie (pa_codmin);
CREATE INDEX IF NOT EXISTS idx_stg_schema_paie_pa_mat ON staging.stg_schema_paie (pa_mat);
CREATE INDEX IF NOT EXISTS idx_stg_schema_paie_period_type ON staging.stg_schema_paie (pa_annee, pa_mois, pa_type);

-- =========================================================
-- Raw indemnity JSON (schema-indemnity.json)
-- =========================================================
CREATE TABLE IF NOT EXISTS staging.stg_schema_indemnity (
    pa_codmin TEXT,
    pa_mat TEXT,
    pa_mois INTEGER,
    pa_annee INTEGER,
    pa_type TEXT,
    pa_sec NUMERIC,
    pa_noml TEXT,
    pa_prenl TEXT,
    pa_sexe TEXT,
    pa_adrl TEXT,
    pa_regcnr TEXT,
    pa_capd TEXT,
    pa_cab TEXT,
    pa_sg TEXT,
    pa_dg TEXT,
    pa_dire TEXT,
    pa_sdir TEXT,
    pa_serv TEXT,
    pa_unite TEXT,
    pa_loca TEXT,
    pa_article TEXT,
    pa_parag TEXT,
    pa_mp TEXT,
    pa_idbank TEXT,
    pa_grd TEXT,
    pa_eche NUMERIC,
    pa_nbrfam TEXT,
    pa_enfits NUMERIC,
    pa_totinf NUMERIC,
    pa_codconj TEXT,
    pa_sitfam TEXT,
    pa_efonc TEXT,
    pa_fonc TEXT,
    pa_indice NUMERIC,
    pa_natu TEXT,
    pa_mutuel TEXT,
    pa_typarmee TEXT,
    pa_salimp NUMERIC,
    pa_salnimp NUMERIC,
    pa_avkm NUMERIC,
    pa_avlog NUMERIC,
    pa_cpe NUMERIC,
    pa_retrait NUMERIC,
    pa_cps NUMERIC,
    pa_capdeces NUMERIC,
    pa_netord NUMERIC,
    pa_netpay NUMERIC,
    pa_rapimp NUMERIC,
    pa_rapni NUMERIC,
    pa_sub NUMERIC,
    pa_sps NUMERIC,
    pa_spl NUMERIC,
    pa_rapsalb NUMERIC,
    pa_brutcnr NUMERIC,
    pa_salbrut NUMERIC,
    load_ts TIMESTAMPTZ DEFAULT now(),
    source_file TEXT
);

-- Common joins/filters before dimension/fact loading
CREATE INDEX IF NOT EXISTS idx_stg_schema_indemnity_pa_loca ON staging.stg_schema_indemnity (pa_loca);
CREATE INDEX IF NOT EXISTS idx_stg_schema_indemnity_pa_codmin ON staging.stg_schema_indemnity (pa_codmin);
CREATE INDEX IF NOT EXISTS idx_stg_schema_indemnity_pa_grd ON staging.stg_schema_indemnity (pa_grd);
CREATE INDEX IF NOT EXISTS idx_stg_schema_indemnity_pa_natu ON staging.stg_schema_indemnity (pa_natu);
CREATE INDEX IF NOT EXISTS idx_stg_schema_indemnity_pa_mat ON staging.stg_schema_indemnity (pa_mat);
CREATE INDEX IF NOT EXISTS idx_stg_schema_indemnity_period_type ON staging.stg_schema_indemnity (pa_annee, pa_mois, pa_type);

-- =========================================================
-- Column-based JSON masters (keys from items[] = lowercase)
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stg_grade (
    codgrd TEXT,
    codcorps TEXT,
    cat TEXT,
    classgrd TEXT,
    efonc TEXT,
    libcgrdl TEXT,
    libcgrda TEXT,
    liblgrdl TEXT,
    liblgrda TEXT,
    agemax TEXT,
    agemin TEXT,
    gprom TEXT,
    typgrd TEXT,
    ageret TEXT,
    defgi TEXT,
    defgv TEXT,
    hcorps TEXT,
    etat_g TEXT,
    natrem TEXT,
    finrec TEXT,
    fin200 TEXT,
    nivdep TEXT,
    load_ts TIMESTAMPTZ DEFAULT now(),
    source_file TEXT
);

CREATE INDEX IF NOT EXISTS idx_stg_grade_codgrd ON staging.stg_grade (codgrd);

CREATE TABLE IF NOT EXISTS staging.stg_nature (
    codnat TEXT,
    typnat TEXT,
    libnatl TEXT,
    libnata TEXT,
    load_ts TIMESTAMPTZ DEFAULT now(),
    source_file TEXT
);

CREATE INDEX IF NOT EXISTS idx_stg_nature_codnat ON staging.stg_nature (codnat);

CREATE TABLE IF NOT EXISTS staging.stg_region (
    coddep TEXT,
    codreg TEXT,
    lib_reg TEXT,
    lib_rega TEXT,
    code_dept TEXT,
    code_region TEXT,
    fichier TEXT,
    codsreg TEXT,
    load_ts TIMESTAMPTZ DEFAULT now(),
    source_file TEXT
);

CREATE INDEX IF NOT EXISTS idx_stg_region_coddep_codreg ON staging.stg_region (coddep, codreg);
CREATE INDEX IF NOT EXISTS idx_stg_region_codreg ON staging.stg_region (codreg);

CREATE TABLE IF NOT EXISTS staging.stg_organisme (
    codetab TEXT,
    cab TEXT,
    sg TEXT,
    dg TEXT,
    dire TEXT,
    sdir TEXT,
    serv TEXT,
    unite TEXT,
    liborgl TEXT,
    liborga TEXT,
    typstruct TEXT,
    codloc TEXT,
    etatdorg TEXT,
    roleorgl TEXT,
    roleorga TEXT,
    deleg TEXT,
    codgouv TEXT,
    centreg TEXT,
    gboprg TEXT,
    gbosprg TEXT,
    gboact TEXT,
    gbouo TEXT,
    load_ts TIMESTAMPTZ DEFAULT now(),
    source_file TEXT
);

CREATE INDEX IF NOT EXISTS idx_stg_organisme_nk ON staging.stg_organisme (codetab, cab, sg, dg, dire, sdir, serv, unite);
CREATE INDEX IF NOT EXISTS idx_stg_organisme_codloc ON staging.stg_organisme (codloc);

CREATE TABLE IF NOT EXISTS staging.stg_indem_def (
    tmi_cind TEXT,
    tmi_arg1 NUMERIC,
    tmi_arg2 NUMERIC,
    tmi_nseq NUMERIC,
    tmi_nval NUMERIC,
    tmi_cnr TEXT,
    tmi_imp TEXT,
    tmi_fil1 TEXT,
    tmi_fil2 TEXT,
    tmi_nat TEXT,
    tmi_nai TEXT,
    tmi_pflag NUMERIC,
    tmi_dpc TEXT,
    tmi_zon TEXT,
    tmi_libc TEXT,
    tmi_libl TEXT,
    tmi_cins TEXT,
    tmi_liba TEXT,
    load_ts TIMESTAMPTZ DEFAULT now(),
    source_file TEXT
);

CREATE INDEX IF NOT EXISTS idx_stg_indem_def_tmi_cind ON staging.stg_indem_def (tmi_cind);
CREATE INDEX IF NOT EXISTS idx_stg_indem_def_tmi_cins ON staging.stg_indem_def (tmi_cins);
