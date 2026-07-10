.PHONY: up migrate explore ingest dbt-deps dbt dbt-parse dbt-dev dagster-dev dev all reset test

up:
	docker compose up -d --build

migrate:
	uv run --env-file .env python -m evd_orchestration.tools.migrate

explore:
	uv run --env-file .env python scripts/infer_schema.py --source $(SOURCE) --sample $(or $(SAMPLE),20)

ingest:
	uv run dagster asset materialize -m evd_orchestration --select bronze_lims_raw

dbt-clean:
	uv run --env-file .env dbt clean --project-dir transform/evd_transform --profiles-dir transform
	
dbt-seed:
	uv run --env-file .env dbt seed --project-dir transform/evd_transform --profiles-dir transform
dbt-deps:
	uv run --env-file .env dbt deps --project-dir transform/evd_transform --profiles-dir transform

dbt:
	uv run --env-file .env dbt build --project-dir transform/evd_transform --profiles-dir transform

dbt-parse:
	uv run --env-file .env dbt parse --project-dir transform/evd_transform --profiles-dir transform

all: up migrate ingest dbt

reset:
	docker compose down -v

test:
	uv run pytest

# ── Local dev (no Docker build) ─────────────────────────────────────────────

dagster-dev:
	mkdir -p $(PWD)/.dagster_home
	DAGSTER_HOME=$(PWD)/.dagster_home uv run dagster dev -m evd_orchestration

dbt-dev:
	uv run --env-file .env dbt build --project-dir transform/evd_transform --profiles-dir transform --target dev

dev: dbt-deps dbt-parse dagster-dev

dbt-refresh: dbt-clean dbt-deps dbt-seed dbt
