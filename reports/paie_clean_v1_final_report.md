# Final Validation Report (paie_clean_v1)

## 1. Repository Cleanup Plan
- Files to keep: 43
- Files to delete: 1
- Files to refactor: 1

## 2. Dataset Profiling Summary
- Total rows: 756018
- Total columns profiled: 59

## 3. Business Rule Validation
- Valid rows: 5072
- Suspicious rows: 750946
- Inconsistent rows: 0
- Duplicate groups (grain): 0

## 4. Reference Coverage
- grade: matched=731570 / 734521 (rate=0.9960)
- nature: matched=756013 / 756013 (rate=1.0000)
- organisme: matched=36530 / 756018 (rate=0.0483)
- region: matched=3244 / 750521 (rate=0.0043)

## 5. Data Contract
- Grain: employee x month x type
- Primary key: employee_id, pa_annee, pa_mois, pa_type
- Contract columns listed: 31

## 6. Quality Gate Script
- Script: et/run_payroll_quality_gate.py
- Mode: reproducible single-command validation

## 7. Validation Report
- Total rows: 756018
- Rows preserved: 756018
- Valid rows: 5072
- Rows with issues: 750946
- Final status: PASS WITH WARNINGS

## 8. Final Verdict
- Dataset readiness: PASS WITH WARNINGS
- Guarantees: all rows preserved; no fabricated values; auditable rule outputs generated.
- Limitations: unmatched reference codes may remain and are reported, not silently altered.

## 9. Ready-for-ETL Confirmation
- Proceed to DW if final status is PASS or PASS WITH WARNINGS and accepted by governance.
