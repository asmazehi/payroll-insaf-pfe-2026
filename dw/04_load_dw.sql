INSERT INTO public.dim_employee (pa_mat, pa_noml, pa_prenl, pa_sexe, pa_datnais, pa_datent)
SELECT DISTINCT
    trim(s.pa_mat),
    NULLIF(trim(s.pa_noml), ''),
    NULLIF(trim(s.pa_prenl), ''),
    NULLIF(trim(s.pa_sexe), ''),
    NULLIF(trim(s.pa_datnais), ''),
    NULLIF(trim(s.pa_datent), '')
FROM (
    SELECT pa_mat, pa_noml, pa_prenl, pa_sexe, pa_datnais, pa_datent FROM staging.stg_paie2015
    UNION ALL
    SELECT pa_mat, pa_noml, pa_prenl, pa_sexe, pa_datnais, pa_datent FROM staging.stg_ind2015
) s
WHERE NULLIF(trim(s.pa_mat), '') IS NOT NULL
ON CONFLICT (pa_mat) DO NOTHING;

INSERT INTO public.dim_temps (pa_annee, pa_mois)
SELECT DISTINCT pa_annee, pa_mois
FROM (
    SELECT pa_annee, pa_mois FROM staging.stg_paie2015
    UNION ALL
    SELECT pa_annee, pa_mois FROM staging.stg_ind2015
) t
WHERE pa_annee IS NOT NULL
  AND pa_mois IS NOT NULL
ON CONFLICT (pa_annee, pa_mois) DO NOTHING;

INSERT INTO public.dim_grade (
    codgrd, codcorps, cat, classgrd, efonc, libcgrdl, libcgrda, liblgrdl, liblgrda,
    agemax, agemin, gprom, typgrd, ageret, defgi, defgv, hcorps, etat_g, natrem, finrec, fin200, nivdep
)
SELECT DISTINCT
    trim(codgrd), codcorps, cat, classgrd, efonc, libcgrdl, libcgrda, liblgrdl, liblgrda,
    agemax, agemin, gprom, typgrd, ageret, defgi, defgv, hcorps, etat_g, natrem, finrec, fin200, nivdep
FROM staging.stg_grade
WHERE NULLIF(trim(codgrd), '') IS NOT NULL
ON CONFLICT (codgrd) DO NOTHING;

INSERT INTO public.dim_nature (codnat, typnat, libnatl, libnata)
SELECT DISTINCT trim(codnat), typnat, libnatl, libnata
FROM staging.stg_nature
WHERE NULLIF(trim(codnat), '') IS NOT NULL
ON CONFLICT (codnat) DO NOTHING;

INSERT INTO public.dim_region (coddep, codreg, lib_reg, lib_rega, code_dept, code_region, fichier, codsreg)
SELECT DISTINCT
    trim(coddep), trim(codreg), lib_reg, lib_rega, code_dept, code_region, fichier, codsreg
FROM staging.stg_region
WHERE NULLIF(trim(coddep), '') IS NOT NULL
  AND NULLIF(trim(codreg), '') IS NOT NULL
ON CONFLICT (coddep, codreg) DO NOTHING;

INSERT INTO public.dim_organisme (
    codetab, cab, sg, dg, dire, sdir, serv, unite,
    liborgl, liborga, typstruct, codloc, etatdorg, roleorgl, roleorga, deleg, codgouv, centreg, gboprg, gbosprg, gboact, gbouo
)
SELECT DISTINCT
    COALESCE(trim(codetab), ''),
    COALESCE(trim(cab), ''),
    COALESCE(trim(sg), ''),
    COALESCE(trim(dg), ''),
    COALESCE(trim(dire), ''),
    COALESCE(trim(sdir), ''),
    COALESCE(trim(serv), ''),
    COALESCE(trim(unite), ''),
    liborgl, liborga, typstruct, codloc, etatdorg, roleorgl, roleorga, deleg, codgouv, centreg, gboprg, gbosprg, gboact, gbouo
FROM staging.stg_organisme
WHERE NULLIF(trim(codetab), '') IS NOT NULL
ON CONFLICT (codetab, cab, sg, dg, dire, sdir, serv, unite) DO NOTHING;

INSERT INTO public.dim_indemnite (
    tmi_cind, tmi_arg1, tmi_arg2, tmi_nseq, tmi_nval, tmi_cnr, tmi_imp, tmi_fil1, tmi_fil2,
    tmi_nat, tmi_nai, tmi_pflag, tmi_dpc, tmi_zon, tmi_libc, tmi_libl, tmi_cins, tmi_liba
)
SELECT DISTINCT
    trim(tmi_cind), tmi_arg1, tmi_arg2, tmi_nseq, tmi_nval, tmi_cnr, tmi_imp, tmi_fil1, tmi_fil2,
    tmi_nat, tmi_nai, tmi_pflag, tmi_dpc, tmi_zon, tmi_libc, tmi_libl, tmi_cins, tmi_liba
FROM staging.stg_indem_def
WHERE NULLIF(trim(tmi_cind), '') IS NOT NULL
ON CONFLICT (tmi_cind, tmi_dpc) DO NOTHING;

INSERT INTO public.fact_paie (
    employee_sk, time_sk, grade_sk, nature_sk, region_sk, organisme_sk,
    pa_mat, pa_annee, pa_mois, pa_type, pa_sec, pa_codmin, pa_dire, pa_article, pa_parag,
    pa_salimp, pa_salnimp, pa_avkm, pa_avlog, pa_cpe, pa_retrait, pa_cps, pa_capdeces,
    pa_netord, pa_netpay, pa_rapimp, pa_rapni, pa_sub, pa_sps, pa_spl, pa_rapsalb, pa_brutcnr, pa_salbrut,
    source_name, load_ts
)
SELECT
    de.employee_sk,
    dt.time_sk,
    dg.grade_sk,
    dn.nature_sk,
    dr.region_sk,
    do2.organisme_sk,
    trim(s.pa_mat) AS pa_mat,
    s.pa_annee,
    s.pa_mois::smallint,
    COALESCE(trim(s.pa_type), ''),
    COALESCE(s.pa_sec, -1),
    COALESCE(trim(s.pa_codmin), ''),
    COALESCE(trim(s.pa_dire), ''),
    COALESCE(trim(s.pa_article), ''),
    COALESCE(trim(s.pa_parag), ''),
    s.pa_salimp,
    s.pa_salnimp,
    s.pa_avkm,
    s.pa_avlog,
    s.pa_cpe,
    s.pa_retrait,
    s.pa_cps,
    s.pa_capdeces,
    s.pa_netord,
    s.pa_netpay,
    s.pa_rapimp,
    s.pa_rapni,
    s.pa_sub,
    s.pa_sps,
    s.pa_spl,
    s.pa_rapsalb,
    s.pa_brutcnr,
    s.pa_salbrut,
    s.source_name,
    s.load_ts
FROM staging.stg_paie2015 s
JOIN public.dim_employee de ON de.pa_mat = trim(s.pa_mat)
JOIN public.dim_temps dt ON dt.pa_annee = s.pa_annee AND dt.pa_mois = s.pa_mois
LEFT JOIN public.dim_grade dg ON dg.codgrd = trim(s.pa_grd)
LEFT JOIN public.dim_nature dn ON dn.codnat = trim(s.pa_natu)
LEFT JOIN public.dim_region dr ON dr.coddep = trim(s.pa_codmin) AND dr.codreg = trim(s.pa_loca)
LEFT JOIN public.dim_organisme do2
       ON do2.codetab = COALESCE(trim(s.pa_codmin), '')
      AND do2.cab = COALESCE(trim(s.pa_cab), '')
      AND do2.sg = COALESCE(trim(s.pa_sg), '')
      AND do2.dg = COALESCE(trim(s.pa_dg), '')
      AND do2.dire = COALESCE(trim(s.pa_dire), '')
      AND do2.sdir = COALESCE(trim(s.pa_sdir), '')
      AND do2.serv = COALESCE(trim(s.pa_serv), '')
      AND do2.unite = COALESCE(trim(s.pa_unite), '')
WHERE NULLIF(trim(s.pa_mat), '') IS NOT NULL
ON CONFLICT (pa_mat, pa_annee, pa_mois, pa_type, pa_sec, pa_codmin, pa_dire, pa_article, pa_parag) DO NOTHING;

DO $$
BEGIN
    RAISE NOTICE 'TODO: fact_indemnite -> dim_indemnite mapping is unclear because ind2015 detected columns do not contain a direct indemnity code like tmi_cind.';
END $$;

INSERT INTO public.fact_indemnite (
    employee_sk, time_sk, grade_sk, nature_sk, region_sk, organisme_sk, indemnite_sk,
    pa_mat, pa_annee, pa_mois, pa_type, pa_sec, pa_codmin, pa_dire, pa_article, pa_parag,
    pa_salimp, pa_salnimp, pa_avkm, pa_avlog, pa_cpe, pa_retrait, pa_cps, pa_capdeces,
    pa_netord, pa_netpay, pa_rapimp, pa_rapni, pa_sub, pa_sps, pa_spl, pa_rapsalb, pa_brutcnr, pa_salbrut,
    source_name, load_ts
)
SELECT
    de.employee_sk,
    dt.time_sk,
    dg.grade_sk,
    dn.nature_sk,
    dr.region_sk,
    do2.organisme_sk,
    NULL::bigint AS indemnite_sk,
    trim(s.pa_mat) AS pa_mat,
    s.pa_annee,
    s.pa_mois::smallint,
    COALESCE(trim(s.pa_type), ''),
    COALESCE(s.pa_sec, -1),
    COALESCE(trim(s.pa_codmin), ''),
    COALESCE(trim(s.pa_dire), ''),
    COALESCE(trim(s.pa_article), ''),
    COALESCE(trim(s.pa_parag), ''),
    s.pa_salimp,
    s.pa_salnimp,
    s.pa_avkm,
    s.pa_avlog,
    s.pa_cpe,
    s.pa_retrait,
    s.pa_cps,
    s.pa_capdeces,
    s.pa_netord,
    s.pa_netpay,
    s.pa_rapimp,
    s.pa_rapni,
    s.pa_sub,
    s.pa_sps,
    s.pa_spl,
    s.pa_rapsalb,
    s.pa_brutcnr,
    s.pa_salbrut,
    s.source_name,
    s.load_ts
FROM staging.stg_ind2015 s
JOIN public.dim_employee de ON de.pa_mat = trim(s.pa_mat)
JOIN public.dim_temps dt ON dt.pa_annee = s.pa_annee AND dt.pa_mois = s.pa_mois
LEFT JOIN public.dim_grade dg ON dg.codgrd = trim(s.pa_grd)
LEFT JOIN public.dim_nature dn ON dn.codnat = trim(s.pa_natu)
LEFT JOIN public.dim_region dr ON dr.coddep = trim(s.pa_codmin) AND dr.codreg = trim(s.pa_loca)
LEFT JOIN public.dim_organisme do2
       ON do2.codetab = COALESCE(trim(s.pa_codmin), '')
      AND do2.cab = COALESCE(trim(s.pa_cab), '')
      AND do2.sg = COALESCE(trim(s.pa_sg), '')
      AND do2.dg = COALESCE(trim(s.pa_dg), '')
      AND do2.dire = COALESCE(trim(s.pa_dire), '')
      AND do2.sdir = COALESCE(trim(s.pa_sdir), '')
      AND do2.serv = COALESCE(trim(s.pa_serv), '')
      AND do2.unite = COALESCE(trim(s.pa_unite), '')
WHERE NULLIF(trim(s.pa_mat), '') IS NOT NULL
ON CONFLICT (pa_mat, pa_annee, pa_mois, pa_type, pa_sec, pa_codmin, pa_dire, pa_article, pa_parag) DO NOTHING;
