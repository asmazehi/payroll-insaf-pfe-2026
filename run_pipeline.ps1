$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (Test-Path ".venv/Scripts/python.exe") {
    $python = ".venv/Scripts/python.exe"
} else {
    $python = "py"
}

function Write-Step([string]$title) {
    Write-Host ""
    Write-Host "========== $title ==========" -ForegroundColor Cyan
}

function Run-SqlFile([string]$relativePath) {
    Write-Step "SQL: $relativePath"
    Get-Content $relativePath -Raw | docker exec -i insaf_pg psql -U insaf -d insaf_dw -v ON_ERROR_STOP=1
}

Write-Step "1) Create staging"
Run-SqlFile "dw/01_create_staging.sql"

Write-Step "2) Create dimensions"
Run-SqlFile "dw/02_create_dimensions.sql"

Write-Step "3) Create facts"
Run-SqlFile "dw/03_create_facts.sql"

Write-Step "4) Clean raw files"
& $python "etl/clean_raw_to_jsonl.py" "data/raw/paie2015.json" "data/clean/paie2015.jsonl" "--progress-mb" "50"
& $python "etl/clean_raw_to_jsonl.py" "data/raw/ind2015.json" "data/clean/ind2015.jsonl" "--progress-mb" "50"
& $python "etl/recover_ind2015.py" "--input" "data/raw/ind2015.json" "--output" "data/clean/ind2015_recovered.jsonl" "--progress-mb" "50"

Write-Step "5) Load staging"
& $python "etl/load_staging.py" "--truncate" "--use-copy"

Write-Step "6) Load DW"
Run-SqlFile "dw/04_load_dw.sql"

Write-Step "7) Validation report"
Run-SqlFile "reports/validation.sql"

Write-Host ""
Write-Host "Pipeline completed successfully." -ForegroundColor Green
