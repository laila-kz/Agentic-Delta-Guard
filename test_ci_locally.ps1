# test_ci_locally.ps1 - Run CI checks locally

$ErrorActionPreference = "Stop"
$separator = "=" * 60
$venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$python = if (Test-Path $venvPython) { $venvPython } else { "python" }
$venvDbt = Join-Path $PSScriptRoot ".venv\Scripts\dbt.exe"
$dbt = if (Test-Path $venvDbt) { $venvDbt } else { "dbt" }

Write-Host $separator -ForegroundColor Cyan
Write-Host "RUNNING CI CHECKS LOCALLY" -ForegroundColor Cyan
Write-Host $separator -ForegroundColor Cyan

Write-Host "`nInstalling compatible test dependencies..." -ForegroundColor Yellow
& $python -m pip install -r requirements.txt
& $python -m pip install --upgrade "typing_extensions>=4.12.2" pytest dbt-core dbt-duckdb
if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed" }

Write-Host "`n[1/6] Validating Data Contract..." -ForegroundColor Yellow
& $python -c "import yaml; yaml.safe_load(open('configs/agent_contract.yaml'))"
if ($LASTEXITCODE -ne 0) { throw "Data Contract Validation Failed" }
Write-Host "Data Contract Validated" -ForegroundColor Green

Write-Host "`n[2/6] Compiling dbt models..." -ForegroundColor Yellow
& $python scripts/sync_contract_to_dbt_vars.py
if ($LASTEXITCODE -ne 0) { throw "Contract-to-dbt sync failed" }
Push-Location dbt_delta_guard
try {
    & $dbt deps --profiles-dir .
    if ($LASTEXITCODE -ne 0) { throw "dbt deps failed" }

    & $dbt compile --profiles-dir .
    if ($LASTEXITCODE -ne 0) { throw "dbt compilation failed" }
}
finally {
    Pop-Location
}
Write-Host "dbt Compilation Passed" -ForegroundColor Green

Write-Host "`n[3/6] Running Chaos Tests..." -ForegroundColor Yellow
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
& $python -m pytest tests/test_chaos_infra.py -v --tb=short
if ($LASTEXITCODE -ne 0) { throw "Chaos Tests Failed" }
Write-Host "Chaos Tests Passed" -ForegroundColor Green

Write-Host "`n[4/6] Testing Sandbox..." -ForegroundColor Yellow
& $python src/delta_guard/sandbox_guard.py
if ($LASTEXITCODE -ne 0) { throw "Sandbox Test Failed" }
Write-Host "Sandbox Test Passed" -ForegroundColor Green

Write-Host "`n[5/6] Running Triage Engine..." -ForegroundColor Yellow
& $python src/delta_guard/triage.py
if ($LASTEXITCODE -ne 0) { throw "Triage Engine Failed" }

if (-not (Test-Path "docs/INCIDENT_LOG.md")) {
    throw "docs/INCIDENT_LOG.md was not created"
}
Write-Host "Triage Engine Passed" -ForegroundColor Green

Write-Host "`n[6/6] Running Benchmark..." -ForegroundColor Yellow
& $python src/delta_guard/benchmark.py
if ($LASTEXITCODE -ne 0) { throw "Benchmark Failed" }

if (-not (Test-Path "docs/reports/storage_benchmark.md")) {
    throw "Benchmark report was not created"
}
Write-Host "Benchmark Completed" -ForegroundColor Green

Write-Host "`n$separator" -ForegroundColor Green
Write-Host "ALL CI CHECKS PASSED!" -ForegroundColor Green
Write-Host $separator -ForegroundColor Green