# Asma Data Understanding

## 1) Executive summary
- `schema-paie.json` is the monthly payroll fact payload (salary, deductions, net) at employee-period level.
- `schema-indemnity.json` is an indemnity-oriented payroll payload with richer org/location/family/payment fields and `pa_type=3` in the sample.
- `grade.json` is the grade/rank master (`CODGRD`) used by payroll via `pa_grd`.
- `nature.json` is employment nature/type master (`CODNAT`) used by payroll via `pa_natu`.
- `region.json` is location master (`CODREG`) used by payroll location codes (`pa_loca`) and ministry context (`pa_codmin`).
- `organisme.json` is organization hierarchy master (`CODETAB`,`CAB`,`SG`,`DG`,`DIRE`,`SDIR`,`SERV`,`UNITE`) used by payroll org fields.
- `indem_def.json` is indemnity catalog (`TMI_CIND`) with tax/CNR/zone/nature flags and labels.
- Core integration path is payroll (`schema-paie` / `schema-indemnity`) to reference dimensions (`grade`, `nature`, `region`, `organisme`).
- Direct join from payroll record to `indem_def` is **UNKNOWN** in available files because no indemnity code field is present in payroll JSON samples.
- `pa_type` is the major behavioral code: `1=PAIE`, `2=PRIME`, `3=INDCOMP`, `4=PAICOMP` (README + schema docs).

---

## 2) Dataset-by-dataset technical understanding

## A. `schema-paie.json`
### Grain (1 record)
One employee payroll record for one month/year and one payroll type (`pa_mat`,`pa_mois`,`pa_annee`,`pa_type`).

### Primary identifier / key fields
- Business key candidate: (`pa_mat`,`pa_mois`,`pa_annee`,`pa_type`)
- Additional partition keys: `pa_codmin`, `pa_grd`, `pa_natu`

### Join fields to other datasets (exact names)
- To `grade.json`: `pa_grd` = `CODGRD`
- To `nature.json`: `pa_natu` = `CODNAT`
- To `organisme.json` (partial in this file):
  - `pa_codmin` = `CODETAB`
  - `pa_dg` = `DG`
  - `pa_dire` = `DIRE`
  - `pa_sdir` = `SDIR`
  - `pa_serv` = `SERV`
  - `UNITE` join is **UNKNOWN** in this file (no `pa_unite` present in sample)
- To `region.json`: **UNKNOWN in this sample** (no `pa_loca` field present in `schema-paie.json` sample)
- To `indem_def.json`: **UNKNOWN** (no indemnity code field such as `tmi_cind` in payroll sample)

### Top important fields (15)
1. `pa_mat` — employee matricule (employee identifier)
2. `pa_codmin` — ministry code
3. `pa_mois` — pay month
4. `pa_annee` — pay year
5. `pa_type` — payroll type code
6. `pa_grd` — grade code
7. `pa_eche` — echelon/step
8. `pa_natu` — employment nature code
9. `pa_salbrut` — gross salary
10. `pa_salimp` — taxable salary
11. `pa_salnimp` — non-taxable salary
12. `pa_sub` — allowances/subventions
13. `pa_cpe` — tax/contribution deduction
14. `pa_retrait` — retirement deduction
15. `pa_netpay` — net amount paid

### Allowed values / important codes
- `pa_type`:
  - `1` = PAIE
  - `2` = PRIME
  - `3` = INDCOMP
  - `4` = PAICOMP
- `pa_sexe`: `1` (male), `2` (female) per schema docs.

### Data quality checks
- Null checks: `pa_mat`,`pa_mois`,`pa_annee`,`pa_type`,`pa_codmin`,`pa_grd`,`pa_natu`,`pa_salbrut`,`pa_netpay`.
- Domain checks:
  - `pa_mois` in [1..12]
  - `pa_type` in {`1`,`2`,`3`,`4`}
  - `pa_sexe` in {`1`,`2`} when present
- Numeric consistency:
  - `pa_salbrut >= 0`, `pa_salimp >= 0`, `pa_salnimp >= 0`, deductions >= 0
  - `pa_netpay <= pa_salbrut` (unless documented exception)
- Duplicate check on (`pa_mat`,`pa_mois`,`pa_annee`,`pa_type`).
- Referential checks: `pa_grd` exists in `CODGRD`, `pa_natu` exists in `CODNAT`.

---

## B. `schema-indemnity.json`
### Grain (1 record)
One employee indemnity-oriented payroll record for one month/year and one payroll type (`pa_mat`,`pa_mois`,`pa_annee`,`pa_type`).

### Primary identifier / key fields
- Business key candidate: (`pa_mat`,`pa_mois`,`pa_annee`,`pa_type`)
- Sample indicates `pa_type = "3"` (INDCOMP)

### Join fields to other datasets (exact names)
- To `grade.json`: `pa_grd` = `CODGRD`
- To `nature.json`: `pa_natu` = `CODNAT`
- To `region.json`: 
  - `pa_loca` = `CODREG`
  - plus ministry filter `pa_codmin` = `CODDEP` (or `CODE_DEPT`)
- To `organisme.json`:
  - `pa_codmin` = `CODETAB`
  - `pa_cab` = `CAB`
  - `pa_sg` = `SG`
  - `pa_dg` = `DG`
  - `pa_dire` = `DIRE`
  - `pa_sdir` = `SDIR`
  - `pa_serv` = `SERV`
  - `pa_unite` = `UNITE`
- To `indem_def.json`: **UNKNOWN** (no indemnity code field present in this JSON sample)

### Top important fields (15)
1. `pa_mat` — employee matricule
2. `pa_codmin` — ministry code
3. `pa_mois` — pay month
4. `pa_annee` — pay year
5. `pa_type` — payroll type (`3` in sample)
6. `pa_loca` — location code
7. `pa_grd` — grade code
8. `pa_natu` — employment nature code
9. `pa_salbrut` — gross amount
10. `pa_sub` — allowances/subventions
11. `pa_cpe` — tax/contribution deduction
12. `pa_retrait` — retirement deduction
13. `pa_netpay` — net amount paid
14. `pa_idbank` — bank account id
15. `pa_mp` — payment mode

### Allowed values / important codes
- `pa_type`: `1` PAIE, `2` PRIME, `3` INDCOMP, `4` PAICOMP
- `pa_sexe`: `1`/`2` per docs
- `pa_mp`: code list is **UNKNOWN** beyond observed `1D` (docs show example only)

### Data quality checks
- Null checks: `pa_mat`,`pa_mois`,`pa_annee`,`pa_type`,`pa_codmin`,`pa_grd`,`pa_natu`,`pa_loca`,`pa_netpay`.
- Domain checks: `pa_mois` [1..12], `pa_type` in {`1`,`2`,`3`,`4`}.
- Numeric checks: salary/deductions non-negative, `pa_netpay <= pa_salbrut` (unless exception).
- Duplicate check on (`pa_mat`,`pa_mois`,`pa_annee`,`pa_type`).
- Referential checks: joins to `grade`,`nature`,`region`,`organisme` all resolve.
- Bank anomaly checks: same `pa_idbank` reused across many distinct `pa_mat`.

---

## C. `grade.json`
### Grain (1 record)
One civil-service grade/rank definition.

### Primary identifier / key fields
- Primary key: `CODGRD`

### Join fields to other datasets (exact names)
- To payroll files: `CODGRD` = `pa_grd`
- Related analytical field: `NIVDEP`, `FINREC`, `AGERET` used in progression/retirement checks.

### Top important fields (15)
1. `CODGRD` — grade code
2. `CODCORPS` — corps code
3. `CAT` — category
4. `CLASSGRD` — grade class
5. `EFONC` — functional echelon
6. `LIBCGRDL` — short French label
7. `LIBLGRDL` — long French label
8. `LIBCGRDA` — short Arabic label
9. `LIBLGRDA` — long Arabic label
10. `AGERET` — retirement age
11. `AGEMIN` — minimum age
12. `AGEMAX` — maximum age
13. `FINREC` — recruitment end echelon
14. `NIVDEP` — starting level/echelon
15. `NATREM` — remuneration nature

### Allowed values / important codes
- `CAT`: observed categories include `1..9` (docs describe hierarchy; lower number = higher rank category).
- `GPROM`: observed `G`, `P`.
- `TYPGRD`: observed `G`.
- `NATREM`: observed value `V` (where populated).

### Data quality checks
- Null/blank: `CODGRD`, labels, `CAT`.
- Uniqueness: `CODGRD` unique.
- Range checks: `AGEMIN <= AGEMAX`, `AGERET` numeric and plausible.
- Consistency: `NIVDEP <= FINREC` when both numeric.
- Referential readiness: all payroll `pa_grd` values must exist in `CODGRD`.

---

## D. `nature.json`
### Grain (1 record)
One employment nature/type code definition.

### Primary identifier / key fields
- Primary key: `CODNAT`

### Join fields to other datasets (exact names)
- To payroll files: `CODNAT` = `pa_natu`

### Top important fields (dataset has 4 fields)
1. `CODNAT` — nature code
2. `TYPNAT` — broad type group
3. `LIBNATL` — French label
4. `LIBNATA` — Arabic label

### Allowed values / important codes
- `CODNAT` observed: `1,2,3,4,5,6,7,8,9,A,B,C`
- `TYPNAT` observed: 
  - `1` civil servants
  - `2` workers
  - `3` contractual/other

### Data quality checks
- Null/blank: `CODNAT`,`TYPNAT`,`LIBNATL`.
- Uniqueness: `CODNAT` unique.
- Domain: `TYPNAT` in {`1`,`2`,`3`}.
- Referential: all payroll `pa_natu` should map to `CODNAT`.

---

## E. `region.json`
### Grain (1 record)
One region/location/facility reference entry.

### Primary identifier / key fields
- Business key candidate: (`CODDEP`,`CODREG`)
- Alternate technical columns exist: `CODE_DEPT`,`CODE_REGION`

### Join fields to other datasets (exact names)
- To payroll (`schema-indemnity.json`): 
  - `pa_loca` = `CODREG`
  - `pa_codmin` = `CODDEP` (or `CODE_DEPT`)
- To `organisme.json`: possible bridge through location semantics via `CODLOC` and `CODREG` is partially aligned, but formal mapping rule is **UNKNOWN**.

### Top important fields (dataset has 8 fields)
1. `CODDEP` — department/ministry code
2. `CODREG` — region/location code
3. `LIB_REG` — French/latin label
4. `LIB_REGA` — Arabic label
5. `CODE_DEPT` — alt department code
6. `CODE_REGION` — alt region code
7. `FICHIER` — file/source identifier
8. `CODSREG` — sub-region code

### Allowed values / important codes
- `CODDEP` observed includes at least `H00`, `S00`, `F00`.
- `CODSREG` observed includes `0` in many rows.

### Data quality checks
- Null checks: `CODDEP`,`CODREG`,`LIB_REG`.
- Uniqueness: (`CODDEP`,`CODREG`) unique.
- Consistency: `CODE_DEPT` should match `CODDEP` where both populated.
- Referential: payroll `pa_loca` should resolve to `CODREG` under matching ministry.

---

## F. `organisme.json`
### Grain (1 record)
One organizational node (ministry, directorate, regional office, etc.) in the hierarchy.

### Primary identifier / key fields
- Composite key: (`CODETAB`,`CAB`,`SG`,`DG`,`DIRE`,`SDIR`,`SERV`,`UNITE`)

### Join fields to other datasets (exact names)
- To payroll:
  - `pa_codmin` = `CODETAB`
  - `pa_cab` = `CAB`
  - `pa_sg` = `SG`
  - `pa_dg` = `DG`
  - `pa_dire` = `DIRE`
  - `pa_sdir` = `SDIR`
  - `pa_serv` = `SERV`
  - `pa_unite` = `UNITE`
- To region context: `CODLOC` may align with `region.CODREG` for many health structures; full rule is **UNKNOWN**.

### Top important fields (15)
1. `CODETAB` — ministry/establishment code
2. `CAB` — cabinet code
3. `SG` — general secretariat code
4. `DG` — general directorate code
5. `DIRE` — directorate code
6. `SDIR` — sub-directorate code
7. `SERV` — service code
8. `UNITE` — unit code
9. `LIBORGL` — French org label
10. `LIBORGA` — Arabic org label
11. `TYPSTRUCT` — structure type
12. `CODLOC` — location code
13. `CODGOUV` — governorate code
14. `DELEG` — delegation flag
15. `ETATDORG` — organization status

### Allowed values / important codes
- `TYPSTRUCT`: observed `A`.
- `DELEG`: observed `O`, `N`.
- `CENTREG`: observed `R` and `C`.

### Data quality checks
- Null/blank checks on key components (`CODETAB`..`UNITE`) and `LIBORGL`.
- Uniqueness check on composite hierarchy key.
- Code domain checks for `DELEG`,`TYPSTRUCT`.
- Referential check from payroll org keys to organisme composite key.

---

## G. `indem_def.json`
### Grain (1 record)
One indemnity/allowance definition code and its calculation/tax/zone properties.

### Primary identifier / key fields
- Primary key: `TMI_CIND`

### Join fields to other datasets (exact names)
- Intended downstream join from payroll indemnity details should use indemnity code to `TMI_CIND`.
- In available payroll JSON samples (`schema-paie.json`, `schema-indemnity.json`), indemnity code field is **UNKNOWN** / not present.

### Top important fields (15)
1. `TMI_CIND` — indemnity code
2. `TMI_LIBC` — short label
3. `TMI_LIBL` — long label
4. `TMI_CINS` — short numeric code
5. `TMI_IMP` — taxable flag
6. `TMI_CNR` — retirement-base flag
7. `TMI_NAT` — indemnity nature (`F`,`V`,`A` observed)
8. `TMI_NAI` — indemnity nature subtype code
9. `TMI_ZON` — zone code (`C`,`N` observed)
10. `TMI_ARG1` — calculation parameter 1
11. `TMI_ARG2` — calculation parameter 2
12. `TMI_PFLAG` — processing flag
13. `TMI_DPC` — effective date
14. `TMI_FIL1` — filter 1 applicability code
15. `TMI_FIL2` — filter 2 applicability code

### Allowed values / important codes
- `TMI_IMP`: observed `0`,`1`
- `TMI_CNR`: observed `0`,`1`
- `TMI_NAT`: observed `F`,`V`,`A`
- `TMI_ZON`: observed `C`,`N`

### Data quality checks
- Null checks: `TMI_CIND`,`TMI_LIBL`,`TMI_IMP`,`TMI_CNR`,`TMI_NAT`.
- Uniqueness: `TMI_CIND` unique.
- Domain checks: `TMI_IMP` in {`0`,`1`}, `TMI_CNR` in {`0`,`1`}, `TMI_NAT` in {`F`,`V`,`A`}.
- Date parsing: `TMI_DPC` parseable date.
- Label/code consistency: if `TMI_CINS` present, consistent with `TMI_CIND` without leading zeros.

---

## 3) Join Map (exact field names)
- Payroll to grade: `schema-paie.pa_grd` / `schema-indemnity.pa_grd` → `grade.CODGRD`
- Payroll to nature: `schema-paie.pa_natu` / `schema-indemnity.pa_natu` → `nature.CODNAT`
- Payroll to region (where location exists): `schema-indemnity.pa_loca` + `schema-indemnity.pa_codmin` → `region.CODREG` + `region.CODDEP`
- Payroll to organisme (full org key):
  - `pa_codmin`→`CODETAB`, `pa_cab`→`CAB`, `pa_sg`→`SG`, `pa_dg`→`DG`, `pa_dire`→`DIRE`, `pa_sdir`→`SDIR`, `pa_serv`→`SERV`, `pa_unite`→`UNITE`
- Payroll to indemnity definition: **UNKNOWN** (no indemnity code field in payroll JSON samples to map to `indem_def.TMI_CIND`)
- Indemnity master code alignment: `indem_def.TMI_CIND` and `indem_def.TMI_CINS` are internal code variants, not directly linked in provided payroll JSON samples

---

## 4) DW implications
### Recommended fact grains
- `fact_paie`: one row per (`pa_mat`,`pa_mois`,`pa_annee`,`pa_type`) from `schema-paie.json`.
- `fact_indemnite`: one row per (`pa_mat`,`pa_mois`,`pa_annee`,`pa_type`) from `schema-indemnity.json`.
- If indemnity-line detail becomes available (with indemnity code), refine to one row per (`pa_mat`,`period`,`indemnity_code`).

### Recommended dimension tables + surrogate keys
- `dim_employee` (from payroll identity fields): surrogate `employee_sk`, natural `pa_mat`.
- `dim_temps`: surrogate `time_sk`, natural (`pa_annee`,`pa_mois`).
- `dim_grade`: surrogate `grade_sk`, natural `CODGRD`.
- `dim_nature`: surrogate `nature_sk`, natural `CODNAT`.
- `dim_region`: surrogate `region_sk`, natural (`CODDEP`,`CODREG`).
- `dim_organisme`: surrogate `organisme_sk`, natural composite (`CODETAB`,`CAB`,`SG`,`DG`,`DIRE`,`SDIR`,`SERV`,`UNITE`).
- `dim_indemnite`: surrogate `indemnite_sk`, natural `TMI_CIND`.

### Unknowns to resolve (document source)
- **UNKNOWN:** payroll field that links to `indem_def.TMI_CIND`.
  - Should be defined in `docs/schema-indemnity-documentation.md` (or upstream source extraction spec).
- **UNKNOWN:** complete `schema-paie` field list (docs mention more fields than the provided sample record).
  - Should be clarified in `docs/schema-paie-documentation.md` and/or source system schema export.
- **UNKNOWN:** strict mapping rule between `organisme.CODLOC` and `region.CODREG` for all ministries.
  - Should be clarified in `docs/organisme-documentation.md` and `docs/region-documentation.md`.

---

## Normalization note for ETL
Reference JSON masters provide column names in uppercase (`columns.name`) while data rows use lowercase keys (`items` entries, e.g., `codgrd`). Standardize casing during staging before joins.