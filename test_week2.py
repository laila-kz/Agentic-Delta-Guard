#!/usr/bin/env python
"""
Week 2 Verification Script
"""

import os
import sys


DBT_DATABASE = "data/bronze/dbt_delta_guard.duckdb"


def test_dbt_installed():
    try:
        import dbt  # noqa: F401
        print("dbt is installed")
        return True
    except ImportError:
        print("dbt not installed")
        return False


def test_duckdb_installed():
    try:
        import duckdb  # noqa: F401
        print("duckdb is installed")
        return True
    except ImportError:
        print("duckdb not installed")
        return False


def test_dbt_project_exists():
    if os.path.exists("dbt_delta_guard/dbt_project.yml"):
        print("dbt_project.yml exists")
        return True
    print("dbt_project.yml not found")
    return False


def test_silver_exists():
    try:
        import duckdb

        connection = duckdb.connect(DBT_DATABASE, read_only=True)
        relation = connection.execute(
            "select table_type from information_schema.tables "
            "where table_schema = 'main' and table_name = 'stg_agent_events'"
        ).fetchone()
        connection.close()
        if relation and relation[0] == "VIEW":
            print("Silver staging view exists")
            return True
    except (ImportError, FileNotFoundError):
        pass
    print("Silver staging view not found")
    return False


def test_gold_exists():
    try:
        import duckdb

        connection = duckdb.connect(DBT_DATABASE, read_only=True)
        relations = connection.execute(
            "select table_name from information_schema.tables "
            "where table_schema = 'main' and table_name in "
            "('fct_agent_activity', 'dim_tool_efficiency')"
        ).fetchall()
        connection.close()
        if {row[0] for row in relations} == {
            "fct_agent_activity",
            "dim_tool_efficiency",
        }:
            print("Gold mart tables exist")
            return True
    except (ImportError, FileNotFoundError):
        pass
    print("Gold mart tables not found")
    return False


def test_quality_report_exists():
    report_path = "docs/reports/quality_audit.md"
    if os.path.exists(report_path):
        print("Quality report exists")
        return True
    print("Quality report not found")
    return False


def main():
    print("=" * 60)
    print("WEEK 2 VERIFICATION")
    print("=" * 60)

    tests = [
        test_dbt_installed,
        test_duckdb_installed,
        test_dbt_project_exists,
        test_silver_exists,
        test_gold_exists,
        test_quality_report_exists,
    ]

    passed = 0
    for test in tests:
        if test():
            passed += 1

    print("=" * 60)
    print(f"Passed {passed}/{len(tests)} tests")

    if passed == len(tests):
        print("Week 2 is complete!")
        return 0

    print("Some tests failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
