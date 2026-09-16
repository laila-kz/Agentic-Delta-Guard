#!/usr/bin/env python
"""
Week 3 Verification Script
"""

import os
import sys


def test_sandbox_module_exists():
    assert os.path.exists("src/delta_guard/sandbox_guard.py"), "sandbox_guard.py not found"
    print("sandbox_guard.py exists")


def test_triage_module_exists():
    assert os.path.exists("src/delta_guard/triage.py"), "triage.py not found"
    print("triage.py exists")


def test_chaos_suite_exists():
    found = os.path.exists("tests/test_contract_validation.py") or os.path.exists("tests/test_chaos_infra.py")
    assert found, "test_contract_validation.py / test_chaos_infra.py not found"
    print("test_contract_validation.py / test_chaos_infra.py exists")


def test_incident_log_exists():
    assert os.path.exists("docs/INCIDENT_LOG.md"), "INCIDENT_LOG.md not found"
    print("INCIDENT_LOG.md exists")


def test_proposed_contract_exists():
    assert os.path.exists("configs/agent_contract_proposed.yaml"), "agent_contract_proposed.yaml not found"
    print("agent_contract_proposed.yaml exists")


def test_sandbox_exists():
    assert os.path.exists("data/gold/agent_sandbox"), "Sandbox not found"
    print("Sandbox exists")


def main():
    print("=" * 60)
    print("WEEK 3 VERIFICATION")
    print("=" * 60)

    tests = [
        test_sandbox_module_exists,
        test_triage_module_exists,
        test_chaos_suite_exists,
        test_sandbox_exists,
        test_incident_log_exists,
        test_proposed_contract_exists,
    ]

    passed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"Failed: {e}")

    print("=" * 60)
    print(f"Passed {passed}/{len(tests)} tests")

    if passed == len(tests):
        print("\nWeek 3 is complete!")
        return 0

    print("\nSome tests failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
