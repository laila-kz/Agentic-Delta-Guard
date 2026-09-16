# Run End-to-End Verification Pipeline for Agentic Delta Guard
$ErrorActionPreference = "Stop"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Agentic Delta Guard: End-to-End Verification Pipeline" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# Step 1: Sync Contract to dbt vars
Write-Host "`n[Step 1/5] Syncing Contract to dbt variables..." -ForegroundColor Yellow
python scripts/sync_contract_to_dbt_vars.py
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to sync contract to dbt variables."
    exit 1
}

# Step 2: Pytest Suite
Write-Host "`n[Step 2/5] Running Pytest Test Suite..." -ForegroundColor Yellow
pytest tests/ -v --tb=short
if ($LASTEXITCODE -ne 0) {
    Write-Error "Pytest suite failed."
    exit 1
}

# Step 3: dbt Models & Tests
Write-Host "`n[Step 3/5] Running dbt Gold Transformations and Validations..." -ForegroundColor Yellow
Push-Location dbt_delta_guard
try {
    dbt run --profiles-dir .
    if ($LASTEXITCODE -ne 0) {
        Write-Error "dbt run failed."
        exit 1
    }
    dbt test --profiles-dir .
    if ($LASTEXITCODE -ne 0) {
        Write-Error "dbt test failed."
        exit 1
    }
}
finally {
    Pop-Location
}

# Step 4: Measure Triage Efficiency
Write-Host "`n[Step 4/5] Measuring Quarantine Triage Efficiency..." -ForegroundColor Yellow
python scripts/measure_triage_efficiency.py
if ($LASTEXITCODE -ne 0) {
    Write-Error "Triage efficiency measurement failed."
    exit 1
}

# Step 5: Storage & Throughput Benchmarks
Write-Host "`n[Step 5/5] Running Storage and Throughput Benchmark..." -ForegroundColor Yellow
python src/delta_guard/benchmark.py
if ($LASTEXITCODE -ne 0) {
    Write-Error "Benchmark generation failed."
    exit 1
}

Write-Host "`n============================================================" -ForegroundColor Green
Write-Host "  ALL VERIFICATIONS PASSED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
