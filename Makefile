.PHONY: help install up down demo demo-local pipeline produce gatekeeper hud query-bronze query-quarantine \
	dbt-deps dbt-run dbt-compile dbt-test quality-report benchmark sandbox triage \
	chaos-test validate test ci-local clean console

PYTHON ?= python
DBT ?= dbt
DBT_DIR := dbt_delta_guard

help:
	@echo "Agentic Delta Guard commands:"
	@echo "  make pipeline         Centralized run — Kafka + Producer + Gatekeeper + Terminal HUD"
	@echo "  make up               Start all services via Docker (Kafka, producer, gatekeeper, console)"
	@echo "  make demo-local       Start all services locally in 1 terminal command (python run_demo.py)"
	@echo "  make demo             Open the live console in the browser (http://localhost:8888)"
	@echo "  make down             Stop all Docker services"
	@echo "  make test             Run pytest + dbt build (full test suite)"
	@echo "  make install          Install Python dependencies locally"

pipeline:
	$(PYTHON) run_pipeline.py

demo-local:
	$(PYTHON) run_demo.py


	@echo "  make console          Start the status server locally (no Docker)"
	@echo "  make produce          Start the sample event producer locally"
	@echo "  make gatekeeper       Start the PySpark streaming gatekeeper locally"
	@echo "  make hud              Open the real-time terminal HUD"
	@echo "  make dbt-run          Build dbt models"
	@echo "  make dbt-test         Run dbt data tests"
	@echo "  make triage           Run deterministic incident triage"
	@echo "  make clean            Remove generated reports and build artifacts"

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install dbt-core dbt-duckdb pytest

up:  ## Start all services — Kafka, producer, gatekeeper, console server
	docker compose up -d --build
	@echo "Waiting for Kafka to be ready..."
	@until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do \
		sleep 3; \
	done
	@echo ""
	@echo "✅  All services are live."
	@echo "   Live console  →  http://localhost:8888"
	@echo "   Kafka UI      →  http://localhost:8080"
	@echo ""

down:  ## Stop all Docker services
	docker compose down

demo:  ## Open the live console in the default browser
	@python -c "import webbrowser; webbrowser.open('http://localhost:8888')"

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

test:  ## Run pytest + dbt build (the full test suite)
	$(PYTHON) -m pytest tests/ -v --tb=short
	cd $(DBT_DIR) && $(DBT) build --profiles-dir .
	@echo "All checks passed."

console:  ## Start the status server locally (without Docker)
	uvicorn delta_guard.status_server:app --host 0.0.0.0 --port 8888 --reload

ci-local:
	powershell -ExecutionPolicy Bypass -File .\test_ci_locally.ps1

clean:
	$(PYTHON) -c "from pathlib import Path; import shutil; paths = ['.pytest_cache', 'dbt_delta_guard/target', 'docs/reports/quality_audit.md', 'docs/reports/quality_report.json', 'docs/reports/storage_benchmark.md', 'docs/INCIDENT_LOG.md', 'configs/agent_contract_proposed.yaml', 'data/gold/agent_sandbox']; [shutil.rmtree(p) if Path(p).is_dir() else Path(p).unlink(missing_ok=True) for p in paths]"


.PHONY: sync-contract verify-e2e

sync-contract:
	python scripts/sync_contract_to_dbt_vars.py

verify-e2e:
	$(PYTHON) scripts/run_e2e_verification.py