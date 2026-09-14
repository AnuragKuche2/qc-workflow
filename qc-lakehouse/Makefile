.PHONY: venv install-baseline check install-spark smoke-local install-databricks smoke-databricks install-dbt smoke-dbt smoke-all

venv:
	uv python pin 3.12
	uv venv

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

install-databricks:
	uv venv .venv-databricks --python 3.12
	uv pip install --python .venv-databricks/bin/python --no-deps -e .
	uv pip install --python .venv-databricks/bin/python python-dotenv databricks-sdk databricks-connect

smoke-databricks:
	.venv-databricks/bin/python scripts/smoke_databricks.py

install-dbt:
	uv add dbt-databricks
	uv add --dev pyyaml

smoke-dbt:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt debug --project-dir dbt/qc_lakehouse
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --select hello_dbt

smoke-all: smoke-local smoke-databricks smoke-dbt
