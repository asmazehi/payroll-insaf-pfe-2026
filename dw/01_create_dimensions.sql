-- INSAF DW - Dimensions
-- Source-aligned columns only (schema docs + JSON files)

CREATE TABLE IF NOT EXISTS dim_employee (
    employee_sk BIGSERIAL PRIMARY KEY,
    pa_mat VARCHAR(20) NOT NULL,
    CONSTRAINT uq_dim_employee_pa_mat UNIQUE (pa_mat)
);

CREATE TABLE IF NOT EXISTS dim_temps (
    time_sk BIGSERIAL PRIMARY KEY,
    pa_annee INTEGER NOT NULL,
    pa_mois SMALLINT NOT NULL,
    CONSTRAINT ck_dim_temps_pa_mois CHECK (pa_mois BETWEEN 1 AND 12),
    CONSTRAINT uq_dim_temps_nk UNIQUE (pa_annee, pa_mois)
);

CREATE TABLE IF NOT EXISTS dim_grade (
    grade_sk BIGSERIAL PRIMARY KEY,
    codgrd VARCHAR(10) NOT NULL,
    codcorps VARCHAR(10),
    cat VARCHAR(5),
    classgrd VARCHAR(10),
    efonc VARCHAR(10),
    libcgrdl VARCHAR(255),
    libcgrda TEXT,
    liblgrdl TEXT,
    liblgrda TEXT,
    agemax VARCHAR(10),
    agemin VARCHAR(10),
    gprom VARCHAR(2),
    typgrd VARCHAR(2),
    ageret VARCHAR(10),
    defgi VARCHAR(20),
    defgv VARCHAR(20),
    hcorps VARCHAR(10),
    etat_g VARCHAR(10),
    natrem VARCHAR(10),
    finrec VARCHAR(10),
    fin200 VARCHAR(10),
    nivdep VARCHAR(10),
    CONSTRAINT uq_dim_grade_codgrd UNIQUE (codgrd)
);

CREATE TABLE IF NOT EXISTS dim_nature (
    nature_sk BIGSERIAL PRIMARY KEY,
    codnat VARCHAR(5) NOT NULL,
    typnat VARCHAR(5),
    libnatl VARCHAR(255),
    libnata TEXT,
    CONSTRAINT uq_dim_nature_codnat UNIQUE (codnat)
);

CREATE TABLE IF NOT EXISTS dim_region (
    region_sk BIGSERIAL PRIMARY KEY,
    coddep VARCHAR(10) NOT NULL,
    codreg VARCHAR(10) NOT NULL,
    lib_reg VARCHAR(255),
    lib_rega TEXT,
    code_dept VARCHAR(10),
    code_region VARCHAR(10),
    fichier VARCHAR(255),
    codsreg VARCHAR(10),
    CONSTRAINT uq_dim_region_nk UNIQUE (coddep, codreg)
);

CREATE TABLE IF NOT EXISTS dim_organisme (
    organisme_sk BIGSERIAL PRIMARY KEY,
    codetab VARCHAR(10) NOT NULL,
    cab VARCHAR(10) NOT NULL,
    sg VARCHAR(10) NOT NULL,
    dg VARCHAR(10) NOT NULL,
    dire VARCHAR(10) NOT NULL,
    sdir VARCHAR(10) NOT NULL,
    serv VARCHAR(10) NOT NULL,
    unite VARCHAR(10) NOT NULL,
    liborgl TEXT,
    liborga TEXT,
    typstruct VARCHAR(10),
    codloc VARCHAR(10),
    etatdorg VARCHAR(10),
    roleorgl TEXT,
    roleorga TEXT,
    deleg VARCHAR(10),
    codgouv VARCHAR(10),
    centreg VARCHAR(10),
    gboprg VARCHAR(10),
    gbosprg VARCHAR(10),
    gboact VARCHAR(10),
    gbouo VARCHAR(10),
    CONSTRAINT uq_dim_organisme_nk UNIQUE (codetab, cab, sg, dg, dire, sdir, serv, unite)
);

CREATE TABLE IF NOT EXISTS dim_indemnite (
    indemnite_sk BIGSERIAL PRIMARY KEY,
    tmi_cind VARCHAR(10) NOT NULL,
    tmi_arg1 NUMERIC(14, 3),
    tmi_arg2 NUMERIC(14, 3),
    tmi_nseq NUMERIC(14, 3),
    tmi_nval NUMERIC(14, 3),
    tmi_cnr VARCHAR(5),
    tmi_imp VARCHAR(5),
    tmi_fil1 VARCHAR(50),
    tmi_fil2 VARCHAR(50),
    tmi_nat VARCHAR(5),
    tmi_nai VARCHAR(10),
    tmi_pflag NUMERIC(14, 3),
    tmi_dpc VARCHAR(20),
    tmi_zon VARCHAR(5),
    tmi_libc VARCHAR(255),
    tmi_libl TEXT,
    tmi_cins VARCHAR(10),
    tmi_liba TEXT,
    CONSTRAINT uq_dim_indemnite_tmi_cind UNIQUE (tmi_cind)
);
