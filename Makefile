.PHONY: up down produce gatekeeper query-bronze query-quarantine dbt-run dbt-test quality-report test-week2 test-week2-clean install-deps clean-week2 help

up:
	docker compose up -d

down:
	docker compose down

produce:
	python src/delta_guard/producer.py

gatekeeper:
	python src/delta_guard/gatekeeper.py

query-bronze:
	python -c "from delta_guard.query_utils import show_bronze; show_bronze()"

query-quarantine:
	python -c "from delta_guard.query_utils import show_quarantine; show_quarantine()"

# Week 2 Commands
install-deps:
	pip install dbt-core dbt-duckdb duckdb pytest jinja2

dbt-run:
	@echo "=== Running dbt models ==="
	cd dbt_delta_guard && dbt run --profiles-dir .

dbt-test:
	@echo "=== Running dbt tests ==="
	cd dbt_delta_guard && dbt test --profiles-dir .

quality-report:
	@echo "=== Generating quality report ==="
	python src/delta_guard/quality_runner.py

test-week2: dbt-run dbt-test quality-report
	@echo ""
	@echo "Week 2 Medallion Pipeline & Quality Tests Passed!"
	@echo "Report available at: docs/reports/quality_audit.md"

test-week2-clean: clean-week2
	$(MAKE) test-week2

clean-week2:
	@echo "Cleaning Week 2 artifacts..."
	rm -rf data/delta_guard.duckdb
	rm -rf data/silver/*
	rm -rf data/gold/*
	rm -rf dbt_delta_guard/target
	rm -rf docs/reports/*

help:
	@echo "Available Week 2 commands:"
	@echo "  make install-deps       - Install dbt dependencies"
	@echo "  make dbt-run            - Run all dbt models"
	@echo "  make dbt-test           - Run all dbt tests"
	@echo "  make quality-report     - Generate quality report"
	@echo "  make test-week2         - Full Week 2 test suite"
	@echo "  make test-week2-clean   - Full test with cleanup"
	@echo "  make clean-week2        - Clean Week 2 artifacts"
