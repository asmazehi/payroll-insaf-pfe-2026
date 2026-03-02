# INSAF — Star Schema Definition

## Fact Tables

### fact_paie
Grain: One employee per month per payroll type.

Natural Key:
(pa_mat, pa_annee, pa_mois, pa_type)

Measures:
- pa_salbrut
- pa_salimp
- pa_salnimp
- pa_sub
- pa_cpe
- pa_retrait
- pa_netpay

Foreign Keys:
- employee_sk
- time_sk
- grade_sk
- nature_sk
- organisme_sk
- region_sk (if applicable)

---

### fact_indemnite (temporary grain)
Grain: One employee per month per payroll type.

Measures:
- pa_salbrut
- pa_sub
- pa_cpe
- pa_retrait
- pa_netpay

NOTE:
Awaiting indemnity code linkage to indem_def.TMI_CIND.

## Dimensions

### dim_employee
Natural key: pa_mat

### dim_temps
Natural key: (pa_annee, pa_mois)

### dim_grade
Natural key: CODGRD

### dim_nature
Natural key: CODNAT

### dim_region
Natural key: (CODDEP, CODREG)

### dim_organisme
Natural key: (CODETAB, CAB, SG, DG, DIRE, SDIR, SERV, UNITE)

### dim_indemnite (future-ready)
Natural key: TMI_CIND