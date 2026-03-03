# ETL Summary (Raw → Clean JSONL → Staging → DW)

Date: 2026-03-03

## 1) Pipeline overview

Current pipeline execution path:

1. **Raw JSON sources**
   - `data/raw/paie2015.json`, `data/raw/ind2015.json`, and reference JSON files.
2. **Clean JSONL generation**
   - `etl/clean_raw_to_jsonl.py` creates line-delimited clean files in `data/clean/`.
   - For indemnities, forensic recovery (`etl/recover_ind2015.py`) is used when needed to maximize salvage quality.
3. **Staging load**
   - `etl/load_staging.py` loads clean JSONL into PostgreSQL staging tables (COPY path preferred).
4. **DW load**
   - `dw/04_load_dw.sql` loads dimensions and facts from staging.
5. **Validation**
   - `reports/validation.sql` checks counts, duplicates, null key joins, and business sanity rules.

---

## 2) Exact row counts (current)

### Staging

| Table | Rows |
|---|---:|
| `staging.stg_paie2015` | 22,867 |
| `staging.stg_ind2015` | 75,699 |
| `staging.stg_grade` | 1,914 |
| `staging.stg_nature` | 12 |
| `staging.stg_region` | 135 |
| `staging.stg_organisme` | 241 |
| `staging.stg_indem_def` | 723 |

### Dimensions (DW)

| Table | Rows |
|---|---:|
| `public.dim_employee` | 15,064 |
| `public.dim_temps` | 121 |
| `public.dim_grade` | 1,914 |
| `public.dim_nature` | 12 |
| `public.dim_region` | 135 |
| `public.dim_organisme` | 241 |
| `public.dim_indemnite` | 723 |

### Facts (DW)

| Table | Rows |
|---|---:|
| `public.fact_paie` | 22,867 |
| `public.fact_indemnite` | 75,699 |

---

## 3) Validation checks and results

### Duplicate business-key groups

- `fact_paie`: **0** duplicate groups
- `fact_indemnite`: **0** duplicate groups

### Null key/join checks

(Counts for `employee_sk`, `time_sk`, `pa_mat`, `pa_annee`, `pa_mois`)

- `fact_paie`: all **0**
- `fact_indemnite`: all **0**

### Business sanity check (`netpay > salbrut`)

- `fact_paie`: **0** invalid rows
- `fact_indemnite`: **0** invalid rows

Overall status: **PASS** for the listed validation checks.

---

## 4) Known limitation: paie salvage scope

Observed result for payroll (`paie2015`):

- Existing clean file count = **22,867** rows.
- Fresh full re-clean probe from raw file also produced = **22,867** rows.
- Loaded counts in staging and DW exactly match this number.

Why we stop additional paie salvage now:

1. Current cleaner already scans the full raw payload and extracts all records matching the payroll structure used by the model.
2. Re-running extraction with the same recovery logic yields identical totals, indicating no additional recoverable valid rows under current rules.
3. More aggressive salvage heuristics would likely increase false positives/noise (non-payroll fragments) with low expected gain.

Decision: **Treat 22,867 as complete under the current recovery strategy** and prioritize data quality/stability over speculative extra extraction.
