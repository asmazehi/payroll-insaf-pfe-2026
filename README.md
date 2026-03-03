# INSAF — ETL & Data Warehouse

This repository contains the finalized ETL + DW phase for the payroll/indemnity BI project.

## Repository layout

- `data/raw/`: raw JSON sources used by ETL
- `data/clean/`: generated clean JSONL files used for staging loads
- `etl/`: cleaning, recovery, and staging load scripts
- `dw/`: SQL scripts for staging/dim/fact creation and DW load
- `reports/`: validation SQL and ETL execution summary
- `run_pipeline.ps1`: end-to-end reproducible pipeline

## Reproducible pipeline

Run the full pipeline from repository root:

```powershell
./run_pipeline.ps1
```

Execution sequence:

1. Create staging (`dw/01_create_staging.sql`)
2. Create dimensions (`dw/02_create_dimensions.sql`)
3. Create facts (`dw/03_create_facts.sql`)
4. Clean raw files into JSONL (`etl/clean_raw_to_jsonl.py` + `etl/recover_ind2015.py`)
5. Load staging (`etl/load_staging.py --truncate --use-copy`)
6. Load DW (`dw/04_load_dw.sql`)
7. Run validation checks (`reports/validation.sql`)

## Project State

### Current dataset coverage

- Payroll (`paie2015`) loaded end-to-end
- Indemnities (`ind2015`) loaded end-to-end
- Reference dimensions loaded from raw reference JSON

### Row counts (current validated state)

- `staging.stg_paie2015`: **22,867**
- `staging.stg_ind2015`: **75,699**
- `public.dim_employee`: **15,064**
- `public.dim_temps`: **121**
- `public.fact_paie`: **22,867**
- `public.fact_indemnite`: **75,699**

Full table counts and checks are documented in `reports/etl_summary.md`.

### Data quality status

- Duplicate business-key groups: **0** (`fact_paie`, `fact_indemnite`)
- Null FK/key join checks: **0**
- Invalid `netpay > salbrut`: **0**

### Known limitation

Payroll salvage reached a plateau at **22,867** rows under current recovery rules.

- Re-cleaning `data/raw/paie2015.json` repeatedly yields the same row count.
- Additional aggressive salvage heuristics are likely to increase false positives with low expected gain.

Decision: keep the current conservative recovery strategy for stability and reproducibility.

## Next phase

The next planned phase is a **Backend Clean Architecture API** to expose DW data for dashboards and downstream analytics services.
