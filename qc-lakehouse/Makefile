.PHONY: venv install install-baseline check install-spark smoke-local install-databricks smoke-databricks install-dbt smoke-dbt install-airflow test-orchestration smoke-all generate-reference-data generate-fact-data dbt-run dbt-test dbt-docs

venv:
	uv python pin 3.12
	uv venv

# Clean-clone install: installs exactly what pyproject.toml/uv.lock already declare, no
# mutation. This is the primary documented setup step - see README.
install:
	uv sync

# NOTE: install-baseline/install-spark/install-dbt use `uv add`, which re-resolves and can
# mutate pyproject.toml/uv.lock. That's fine for adding a genuinely new dependency, but it's
# the wrong tool for clean-clone setup (use `make install` instead) since it can leave a
# fresh checkout with an unexpectedly dirty tree.
install-baseline:
	uv add --dev ruff pytest
	uv add python-dotenv

check:
	uv run ruff check .
	uv run pytest -q

install-spark:
	uv add pyspark delta-spark

smoke-local:
	uv run python scripts/smoke_local.py

# Pinned to the exact databricks-connect/databricks-sdk versions confirmed working against
# Databricks Free Edition's serverless runtime via the real smoke test (Task 6). The design
# spec (docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md, section 6)
# flagged this version as an open question to resolve during this sub-project rather than
# leave floating. Bumping these versions should be a deliberate, tested decision - not an
# incidental drift from an unpinned `uv pip install`.
install-databricks:
	uv venv .venv-databricks --python 3.12
	uv pip install --python .venv-databricks/bin/python --no-deps -e .
	uv pip install --python .venv-databricks/bin/python python-dotenv "databricks-sdk==0.139.*" "databricks-connect==19.1.*"

smoke-databricks:
	.venv-databricks/bin/python scripts/smoke_databricks.py

install-dbt:
	uv add dbt-databricks
	uv add --dev pyyaml

smoke-dbt:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt debug --project-dir dbt/qc_lakehouse
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --select hello_dbt

# Isolated venv for Airflow tooling, same reasoning as install-databricks's
# .venv-databricks: Airflow's own dependency constraints are notorious for conflicting with
# an unrelated project's pinned pyspark/dbt-databricks versions, so it never goes in the main
# venv. Airflow itself runs in Docker (see orchestration/docker-compose.yml) - this venv
# exists only so the DAG file can be structurally validated locally via pytest without
# needing Docker running.
install-airflow:
	uv venv .venv-airflow --python 3.12
	uv pip install --python .venv-airflow/bin/python apache-airflow apache-airflow-providers-databricks

# Structural DAG validation only (DagBag import + task/dependency checks) - lives outside the
# main `tests/` dir and the main `make check` because apache-airflow is not installed in the
# main venv. See orchestration/tests/test_dag.py.
test-orchestration:
	.venv-airflow/bin/python -m pytest orchestration/tests -q

smoke-all: smoke-local smoke-databricks smoke-dbt

# Writes real reference data to qc_dev.bronze_source on live Databricks - not a throwaway
# smoke test like smoke-databricks. Uses .venv-databricks (same as smoke-databricks)
# because it needs databricks-connect for the serverless session.
generate-reference-data:
	.venv-databricks/bin/python scripts/generate_reference_data.py

# Writes real fact data to qc_dev.bronze_source on live Databricks. Requires
# generate-reference-data to have been run first (this reads the reference tables it
# wrote). Uses .venv-databricks, same as generate-reference-data.
generate-fact-data:
	.venv-databricks/bin/python scripts/generate_fact_data.py

# Real medallion work against qc_dev.silver/qc_dev.gold - never the smoke-test `dev` target.
dbt-run:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev

dbt-test:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev

dbt-docs:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt docs generate --project-dir dbt/qc_lakehouse --target qc_dev
