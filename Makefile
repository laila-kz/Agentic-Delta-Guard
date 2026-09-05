.PHONY: up down produce gatekeeper query-bronze query-quarantine

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
