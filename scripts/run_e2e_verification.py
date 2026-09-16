#!/usr/bin/env python
"""
run_e2e_verification.py — Universal Cross-Platform End-to-End Test Harness.

Executes all 5 stages of verification for Agentic Delta Guard:
1. Contract Synchronization with dbt variables
2. Unit, Schema, and Chaos Pytest Suites
3. dbt Gold Transformation Models and Data Quality Tests
4. Incident Quarantine Triage Efficiency Measurement (MTTR)
5. Storage Integrity and Sub-Millisecond Throughput Benchmarks
"""

import os
import subprocess
import sys
import time

# Ensure safe UTF-8 terminal encoding on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

CYAN = "\033[96m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def print_banner(msg: str, color: str = CYAN):
    line = "=" * 62
    print(f"\n{color}{line}\n  {msg}\n{line}{RESET}\n")


def run_step(step_num: int, total_steps: int, title: str, cmd: list[str], cwd: str = PROJECT_ROOT):
    print(f"{YELLOW}{BOLD}[Step {step_num}/{total_steps}] >> {title}...{RESET}")
    start_time = time.time()
    
    result = subprocess.run(cmd, cwd=cwd)
    duration = time.time() - start_time
    
    if result.returncode != 0:
        print(f"\n{RED}[X] FAILED: {title} (Exit code: {result.returncode}, Duration: {duration:.2f}s){RESET}\n")
        sys.exit(result.returncode)
    else:
        print(f"{GREEN}[OK] Completed in {duration:.2f}s{RESET}\n")


def main():
    print_banner("Agentic Delta Guard: Full End-to-End Verification")

    # Step 1: Sync Contract
    run_step(
        1, 5,
        "Syncing Data Contract to dbt Variables",
        [sys.executable, os.path.join("scripts", "sync_contract_to_dbt_vars.py")]
    )

    # Step 2: Pytest Suite
    run_step(
        2, 5,
        "Running Pytest Unit, Schema, and Chaos Test Suite",
        [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"]
    )

    # Step 3: dbt Run & Test
    dbt_dir = os.path.join(PROJECT_ROOT, "dbt_delta_guard")
    run_step(
        3, 5,
        "Running dbt Transformations and Data Quality Tests",
        ["dbt", "build", "--profiles-dir", "."],
        cwd=dbt_dir
    )

    # Step 4: Measure Triage Efficiency
    run_step(
        4, 5,
        "Measuring Automated Incident Triage Efficiency (MTTR)",
        [sys.executable, os.path.join("scripts", "measure_triage_efficiency.py")]
    )

    # Step 5: Storage & Throughput Benchmark
    run_step(
        5, 5,
        "Generating Storage and Throughput Benchmark Reports",
        [sys.executable, os.path.join("src", "delta_guard", "benchmark.py")]
    )

    print_banner("ALL 5 END-TO-END VERIFICATIONS PASSED SUCCESSFULLY!", GREEN)


if __name__ == "__main__":
    main()
