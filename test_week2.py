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
    except ImportError:
        assert False, "dbt not installed"


def test_duckdb_installed():
    try:
        import duckdb  # noqa: F401
        print("duckdb is installed")
    except ImportError:
        assert False, "duckdb not installed"


def test_dbt_project_exists():
    assert os.path.exists("dbt_delta_guard/dbt_project.yml"), "dbt_project.yml not found"
    print("dbt_project.yml exists")


def test_silver_exists():
    import duckdb

    connection = duckdb.connect(DBT_DATABASE, read_only=True)
    relation = connection.execute(
        "select table_type from information_schema.tables "
        "where table_schema = 'main' and table_name = 'stg_agent_events'"
    ).fetchone()
    connection.close()
    assert relation and relation[0] == "VIEW", "Silver staging view not found"
    print("Silver staging view exists")


def test_gold_exists():
    import duckdb

    connection = duckdb.connect(DBT_DATABASE, read_only=True)
    relations = connection.execute(
        "select table_name from information_schema.tables "
        "where table_schema = 'main' and table_name in "
        "('fct_agent_activity', 'dim_tool_efficiency')"
    ).fetchall()
    connection.close()
    found_tables = {row[0] for row in relations}
    assert found_tables == {"fct_agent_activity", "dim_tool_efficiency"}, "Gold mart tables not found"
    print("Gold mart tables exist")


def test_quality_report_exists():
    report_path = "docs/reports/quality_audit.md"
    assert os.path.exists(report_path), "Quality report not found"
    print("Quality report exists")


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
        try:
            test()
            passed += 1
        except (AssertionError, Exception) as e:
            print(f"Failed: {e}")

    print("=" * 60)
    print(f"Passed {passed}/{len(tests)} tests")

    if passed == len(tests):
        print("Week 2 is complete!")
        return 0

    print("Some tests failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
