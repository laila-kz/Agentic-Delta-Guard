.PHONY: help install up down produce gatekeeper hud query-bronze query-quarantine \
	dbt-deps dbt-run dbt-compile dbt-test quality-report benchmark sandbox triage \
	chaos-test validate test ci-local clean

PYTHON ?= python
DBT ?= dbt
DBT_DIR := dbt_delta_guard

help:
	@echo "Agentic Delta Guard commands:"
	@echo "  make install          Install runtime and development dependencies"
	@echo "  make up               Start Kafka, ZooKeeper, and Kafka UI"
	@echo "  make down             Stop Docker services"
	@echo "  make produce          Start the sample event producer"
	@echo "  make gatekeeper       Start the PySpark streaming gatekeeper"
	@echo "  make hud              Open the real-time terminal HUD"
	@echo "  make dbt-compile      Install packages and compile dbt"
	@echo "  make dbt-run          Build dbt models"
	@echo "  make dbt-test         Run dbt data tests"
	@echo "  make quality-report   Generate the dbt quality report"
	@echo "  make benchmark        Generate the storage benchmark report"
	@echo "  make sandbox          Run sandbox assertions"
	@echo "  make triage           Run deterministic incident triage"
	@echo "  make validate         Validate the data contract and Python tests"
	@echo "  make test             Run the complete repository test set"
	@echo "  make ci-local         Run the Windows local CI script"
	@echo "  make clean            Remove generated reports and build artifacts"

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install dbt-core dbt-duckdb pytest

up:
	docker compose up -d

down:
	docker compose down

produce:
	$(PYTHON) src/delta_guard/producer.py

gatekeeper:
	$(PYTHON) src/delta_guard/gatekeeper.py

hud:
	$(PYTHON) src/delta_guard/hud.py

query-bronze:
	$(PYTHON) -c "from src.delta_guard.query_utils import show_bronze; show_bronze()"

query-quarantine:
	$(PYTHON) -c "from src.delta_guard.query_utils import show_quarantine; show_quarantine()"

dbt-deps:
	cd $(DBT_DIR) && $(DBT) deps --profiles-dir .

dbt-compile: dbt-deps
	cd $(DBT_DIR) && $(DBT) compile --profiles-dir .

dbt-run: dbt-deps
	cd $(DBT_DIR) && $(DBT) run --profiles-dir .

dbt-test: dbt-deps
	cd $(DBT_DIR) && $(DBT) test --profiles-dir .

quality-report:
	$(PYTHON) src/delta_guard/quality_runner.py

benchmark:
	$(PYTHON) src/delta_guard/benchmark.py

sandbox:
	$(PYTHON) src/delta_guard/sandbox_guard.py

triage:
	$(PYTHON) src/delta_guard/triage.py

chaos-test:
	$(PYTHON) -m pytest tests/test_chaos_infra.py -v --tb=short

validate:
	$(PYTHON) -c "import yaml; yaml.safe_load(open('configs/agent_contract.yaml'))"
	$(PYTHON) -m pytest tests/test_chaos_infra.py -v --tb=short

test: validate dbt-compile dbt-test quality-report sandbox triage benchmark
	@echo "All validation, modeling, governance, and reporting checks passed."

ci-local:
	powershell -ExecutionPolicy Bypass -File .\test_ci_locally.ps1

clean:
	$(PYTHON) -c "from pathlib import Path; import shutil; paths = ['.pytest_cache', 'dbt_delta_guard/target', 'docs/reports/quality_audit.md', 'docs/reports/quality_report.json', 'docs/reports/storage_benchmark.md', 'docs/INCIDENT_LOG.md', 'configs/agent_contract_proposed.yaml', 'data/gold/agent_sandbox']; [shutil.rmtree(p) if Path(p).is_dir() else Path(p).unlink(missing_ok=True) for p in paths]"


.PHONY: sync-contract

sync-contract:
	python scripts/sync_contract_to_dbt_vars.py