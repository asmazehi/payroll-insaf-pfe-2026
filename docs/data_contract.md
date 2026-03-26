# Data Contract

## Scope

This contract defines the payroll type-1 curated layer used as input to DW modeling.

## Grain

One row per employee, per year, per month, per payroll type.

Business key:
- employee_id
- pa_annee
- pa_mois
- pa_type

## Core Rules

- All source payroll type-1 rows are preserved.
- No fabricated values are introduced.
- Malformed values are repaired only when deterministic.
- Unrecoverable fields remain explicit as unresolved/unknown.

## Dimension Join Policy

- employee: mapped from employee_id
- grade: strict code match
- nature: strict code match
- time: strict year/month match
- organisme: normalized composite mapping from organizational code tokens
- region: strict-only mapping from pa_codmin + pa_loca

## Region Unknown Policy

Region mapping is strict and semantic:
- strict match: real region_key
- no strict match: Unknown region_key (0) in DW-safe dimension

No codreg=000 fallback is allowed.

## Canonical References

- data/clean/fact_paie_src.jsonl
- data/clean/dim_employee_src.jsonl
- data/clean/dim_region_src.jsonl
- data/clean/map_region.jsonl
- data/clean/map_organisme.jsonl
- reports/paie_dw_mapping_layer_report.json
