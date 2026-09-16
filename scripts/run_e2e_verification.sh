#!/usr/bin/env bash
set -euo pipefail

echo "============================================================"
echo "  🛡️ Agentic Delta Guard: End-to-End Verification Pipeline"
echo "============================================================"

# Step 1: Sync Contract to dbt vars
echo -e "\n[Step 1/5] 🔄 Syncing Contract to dbt variables..."
python scripts/sync_contract_to_dbt_vars.py

# Step 2: Pytest Suite
echo -e "\n[Step 2/5] 🧪 Running Pytest Test Suite..."
pytest tests/ -v --tb=short

# Step 3: dbt Models & Tests
echo -e "\n[Step 3/5] 📊 Running dbt Gold Transformations and Validations..."
(cd dbt_delta_guard && dbt build --profiles-dir .)

# Step 4: Measure Triage Efficiency
echo -e "\n[Step 4/5] 🧠 Measuring Quarantine Triage Efficiency..."
python scripts/measure_triage_efficiency.py

# Step 5: Storage & Throughput Benchmarks
echo -e "\n[Step 5/5] 📈 Running Storage and Throughput Benchmark..."
python src/delta_guard/benchmark.py

echo -e "\n============================================================"
echo "  ✅ ALL VERIFICATIONS PASSED SUCCESSFULLY!"
echo "============================================================"
