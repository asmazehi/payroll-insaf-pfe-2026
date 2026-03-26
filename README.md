# Payroll ETL (Clean-Slate Rebuild)

This repository is currently focused on rebuilding the payroll cleaning pipeline from scratch.

Scope of this phase:
- Source dataset: `data/raw/paie2015.json`
- Domain: payroll only
- Filter: `pa_type == "1"`
- Indemnity processing: intentionally out of scope for now

## Goals

The pipeline is designed to produce a clean and trustworthy payroll dataset while preserving information:
- Keep all payroll type-1 rows
- Preserve valid raw values
- Repair malformed tokens when structure allows safe recovery
- Do not invent unsupported values
- Flag unresolved fields explicitly instead of dropping rows
- Keep corrections traceable at field level

## Current Script Layout

- ETL and preparation scripts: `etl/`
- DW scaffolding folders: `dw/`
- Final curated reports: `reports/`
- Technical notes and contract docs: `docs/`

Primary scripts:
- Builder: `etl/build_payroll_dataset.py`
- Quality gate: `etl/run_payroll_quality_gate.py`
- DW input finalization: `etl/finalize_paie_dw_input_layer.py`
- Fact-to-dimension mapping bridges: `etl/build_fact_dimension_mappings.py`

Main behavior:
- Streams large raw payload without loading the full file in memory
- Parses malformed numeric tokens such as decimal commas (`1923,452 -> 1923.452`)
- Normalizes date fields from `dd/mm/yy` to ISO `yyyy-mm-dd` when valid
- Normalizes code and text whitespace conservatively
- Adds quality metadata per row:
	- `dq_unresolved_fields`
	- `dq_has_unresolved`
	- `dq_trace_count`

## How To Run

From repository root:

```powershell
.venv/Scripts/python.exe etl/build_payroll_dataset.py
```

Optional development mode (sample only):

```powershell
.venv/Scripts/python.exe etl/build_payroll_dataset.py --max-rows 10000
```

Run full validation and finalization reports:

```powershell
.venv/Scripts/python.exe etl/run_payroll_quality_gate.py
.venv/Scripts/python.exe etl/finalize_paie_dw_input_layer.py
.venv/Scripts/python.exe etl/build_fact_dimension_mappings.py
```

## Outputs

- Final production datasets: `data/clean/*.jsonl`
- Fact-to-dimension bridges: `data/clean/map_region.jsonl`, `data/clean/map_organisme.jsonl`
- DW-safe region dimension (with Unknown member): `data/clean/dim_region_src.jsonl`
- Quality and mapping reports: `reports/paie_clean_v1_*.json`, `reports/paie_dw_*.json`
- Technical notes and contracts: `docs/technical_notes.md`, `docs/data_contract.md`

## Data Quality Contract

For each payroll type-1 row:
- Row is preserved in output
- Valid fields are preserved
- Malformed fields are repaired when confidently recoverable
- If unrecoverable, only that field is marked unresolved

No blind replacement policy:
- No blanket replacement with `0`
- No blanket replacement with `null`
- Legitimate zeros from source are preserved

## Notes

- Reference files (`grade`, `nature`, `organisme`, `region`) are used only for enrichment labels and validation context in this phase.
- Region mapping is strict by design. No fallback assignment to synthetic/default real regions is allowed.
- Unmatched region rows are handled as Unknown in DW-safe outputs.
