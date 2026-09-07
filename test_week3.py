#!/usr/bin/env python
"""
Week 3 Verification Script
"""

import os
import sys


def test_sandbox_module_exists():
    if os.path.exists("src/delta_guard/sandbox_guard.py"):
        print("sandbox_guard.py exists")
        return True
    print("sandbox_guard.py not found")
    return False


def test_triage_module_exists():
    if os.path.exists("src/delta_guard/triage.py"):
        print("triage.py exists")
        return True
    print("triage.py not found")
    return False


def test_chaos_suite_exists():
    found = False
    if os.path.exists("tests/test_contract_validation.py"):
        print("test_contract_validation.py exists")
        found = True
    if os.path.exists("tests/test_chaos_infra.py"):
        print("test_chaos_infra.py exists")
        found = True
    if not found:
        print("test_contract_validation.py / test_chaos_infra.py not found")
    return found


def test_incident_log_exists():
    if os.path.exists("docs/INCIDENT_LOG.md"):
        print("INCIDENT_LOG.md exists")
        return True
    print("INCIDENT_LOG.md not found")
    return False


def test_proposed_contract_exists():
    if os.path.exists("configs/agent_contract_proposed.yaml"):
        print("agent_contract_proposed.yaml exists")
        return True
    print("agent_contract_proposed.yaml not found")
    return False


def test_sandbox_exists():
    if os.path.exists("data/gold/agent_sandbox"):
        print("Sandbox exists")
        return True
    print("Sandbox not found")
    return False


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
        if test():
            passed += 1

    print("=" * 60)
    print(f"Passed {passed}/{len(tests)} tests")

    if passed == len(tests):
        print("\nWeek 3 is complete!")
        return 0

    print("\nSome tests failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())