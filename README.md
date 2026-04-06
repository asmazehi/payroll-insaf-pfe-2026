# INSAF Payroll Intelligence Platform

Production-grade payroll data pipeline for the Tunisian government payroll system.

## Project Phases

| Phase | Status | Description |
|---|---|---|
| **1 — BI** | 🔨 In progress | Two DWs (payroll + indemnities), Power BI dashboards |
| **2 — Data Science** | Planned | Payroll forecasting, anomaly detection |
| **3 — Platform** | Planned | Web app: upload → ETL → DW → dashboard (automated) |
| **4 — Deployment** | Planned | Docker on company instance or AWS |

---

## Two Data Warehouses (Star Schema)

Both fact tables share the same 6 dimension tables.

### DW1 — Payroll (`fact_paie`)
- **Source**: `data/raw/paie2015.json`
- **Filter**: `pa_type = "1"`
- **Grain**: one row per (employee × month × pa_type)
- **~756,000 rows**

### DW2 — Indemnities (`fact_indem`)
- **Source**: `data/raw/ind2015.json`
- **Filter**: `pa_type = "3"`
- **Grain**: one row per (employee × month × pa_type)
- **~87,000 rows**
- **Extra dimension**: `dim_indemnite` (from `indem_def.json`)

### Shared Dimensions
`dim_employee` · `dim_grade` · `dim_nature` · `dim_organisme` · `dim_region` · `dim_temps`

---

## Running the ETL

```bash
# Install dependencies
pip install -r requirements.txt

# Run DW1 pipeline (payroll type 1)
python -m etl.pipeline_paie

# Run DW2 pipeline (indemnities type 3)
python -m etl.pipeline_indem
```

Both pipelines write clean JSONL to `data/clean/` and a JSON report to `reports/`.

---

## Loading the DW (PostgreSQL)

```bash
psql -d insaf_dw -f dw/sql/01_schema.sql
psql -d insaf_dw -f dw/sql/02_shared_dimensions.sql
psql -d insaf_dw -f dw/sql/03_fact_paie.sql
psql -d insaf_dw -f dw/sql/04_fact_indem.sql
# Load JSONL into staging via \copy, then:
psql -d insaf_dw -f dw/sql/05_load_dimensions.sql
psql -d insaf_dw -f dw/sql/06_load_facts.sql
psql -d insaf_dw -f dw/sql/07_validate.sql
```

See `dw/sql/05_load_dimensions.sql` for the exact `\copy` commands.

---

## ETL Module Structure

```
etl/
├── core/
│   ├── config.py          # All paths, constants, DB config (env-var driven)
│   └── logger.py          # Structured JSON logging with run_id
├── ingestion/
│   └── readers.py         # Universal reader: JSON/JSONL/CSV/Excel
│                          #   + fixes malformed JSON (comma decimal in ind2015)
├── cleaning/
│   ├── encoding.py        # Arabic CP1256 mojibake fix
│   └── normalizer.py      # Dates (DD/MM/YY→ISO), decimals, codes, names
├── mapping/
│   ├── grade.py           # pa_grd    → dim_grade
│   ├── nature.py          # pa_natu   → dim_nature
│   ├── organisme.py       # (codmin, dire) → dim_organisme  (tiered match)
│   ├── region.py          # pa_codmin → dim_region  (conservative)
│   └── indemnite.py       # indem_def → dim_indemnite
├── pipeline_paie.py       # DW1 orchestrator
└── pipeline_indem.py      # DW2 orchestrator
```

---

## Known Limitations (Documented)

| Issue | Impact | Handling |
|---|---|---|
| `pa_loca` has no crosswalk to region.json | Region match ~1% | Unknown member (sk=0) |
| Organisme join uses partial key (codmin+dire) | Org match ~5-10% | Tiered matching, Unknown fallback |
| `ind2015.json` has invalid JSON (French comma decimals) | Cannot `json.load()` directly | Fixed in `readers.py` via regex |
| Arabic labels corrupted (CP1256 read as Latin-1) | Unreadable Arabic text | Fixed in `cleaning/encoding.py` |

---

## Data Contract

- **Never invent data** — NULL stays NULL, 0 stays 0, never swapped
- **Preserve all rows** — no rows dropped; issues flagged via `dq_*` columns
- **Unknown members** — unmatched dimension refs get `sk = 0` (explicit, not NULL)
- **Grain enforced** — PRIMARY KEY on (employee_sk, time_sk, pa_type)
