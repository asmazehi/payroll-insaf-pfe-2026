DROP TABLE IF EXISTS public.dim_employee CASCADE;
CREATE TABLE public.dim_employee (
    employee_sk bigserial PRIMARY KEY,
    pa_mat text NOT NULL UNIQUE,
    pa_noml text,
    pa_prenl text,
    pa_sexe text,
    pa_datnais text,
    pa_datent text,
    created_ts timestamptz NOT NULL DEFAULT now()
);

DROP TABLE IF EXISTS public.dim_temps CASCADE;
CREATE TABLE public.dim_temps (
    time_sk bigserial PRIMARY KEY,
    pa_annee integer NOT NULL,
    pa_mois smallint NOT NULL,
    UNIQUE (pa_annee, pa_mois)
);

DROP TABLE IF EXISTS public.dim_grade CASCADE;
CREATE TABLE public.dim_grade (
    grade_sk bigserial PRIMARY KEY,
    codgrd text NOT NULL UNIQUE,
    codcorps text,
    cat text,
    classgrd text,
    efonc text,
    libcgrdl text,
    libcgrda text,
    liblgrdl text,
    liblgrda text,
    agemax text,
    agemin text,
    gprom text,
    typgrd text,
    ageret text,
    defgi text,
    defgv text,
    hcorps text,
    etat_g text,
    natrem text,
    finrec text,
    fin200 text,
    nivdep text,
    created_ts timestamptz NOT NULL DEFAULT now()
);

DROP TABLE IF EXISTS public.dim_nature CASCADE;
CREATE TABLE public.dim_nature (
    nature_sk bigserial PRIMARY KEY,
    codnat text NOT NULL UNIQUE,
    typnat text,
    libnatl text,
    libnata text,
    created_ts timestamptz NOT NULL DEFAULT now()
);

DROP TABLE IF EXISTS public.dim_region CASCADE;
CREATE TABLE public.dim_region (
    region_sk bigserial PRIMARY KEY,
    coddep text NOT NULL,
    codreg text NOT NULL,
    lib_reg text,
    lib_rega text,
    code_dept text,
    code_region text,
    fichier text,
    codsreg text,
    created_ts timestamptz NOT NULL DEFAULT now(),
    UNIQUE (coddep, codreg)
);

DROP TABLE IF EXISTS public.dim_organisme CASCADE;
CREATE TABLE public.dim_organisme (
    organisme_sk bigserial PRIMARY KEY,
    codetab text NOT NULL,
    cab text NOT NULL,
    sg text NOT NULL,
    dg text NOT NULL,
    dire text NOT NULL,
    sdir text NOT NULL,
    serv text NOT NULL,
    unite text NOT NULL,
    liborgl text,
    liborga text,
    typstruct text,
    codloc text,
    etatdorg text,
    roleorgl text,
    roleorga text,
    deleg text,
    codgouv text,
    centreg text,
    gboprg text,
    gbosprg text,
    gboact text,
    gbouo text,
    created_ts timestamptz NOT NULL DEFAULT now(),
    UNIQUE (codetab, cab, sg, dg, dire, sdir, serv, unite)
);

DROP TABLE IF EXISTS public.dim_indemnite CASCADE;
CREATE TABLE public.dim_indemnite (
    indemnite_sk bigserial PRIMARY KEY,
    tmi_cind text NOT NULL,
    tmi_arg1 numeric(18,6),
    tmi_arg2 numeric(18,6),
    tmi_nseq numeric(18,6),
    tmi_nval numeric(18,6),
    tmi_cnr text,
    tmi_imp text,
    tmi_fil1 text,
    tmi_fil2 text,
    tmi_nat text,
    tmi_nai text,
    tmi_pflag numeric(18,6),
    tmi_dpc text,
    tmi_zon text,
    tmi_libc text,
    tmi_libl text,
    tmi_cins text,
    tmi_liba text,
    created_ts timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tmi_cind, tmi_dpc)
);
