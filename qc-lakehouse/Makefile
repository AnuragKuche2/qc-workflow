.PHONY: venv install install-baseline check install-spark smoke-local install-databricks smoke-databricks install-dbt smoke-dbt install-airflow test-orchestration smoke-all generate-reference-data generate-fact-data dbt-run dbt-test dbt-docs bundle-validate bundle-deploy

venv:
	uv python pin 3.12
	uv venv

# Clean-clone install: installs exactly what pyproject.toml/uv.lock already declare, no
# mutation. Base dependencies are deliberately minimal (just python-dotenv) - pyspark,
# delta-spark, and dbt-databricks live in optional-dependency groups (see install-spark/
# install-dbt below) so the qc_lakehouse wheel built for Databricks Jobs (databricks.yml)
# doesn't drag in a pyspark pin that conflicts with serverless compute's own immutable
# package constraints, or an unused dbt-databricks tree. This alone is NOT the full local
# dev setup - see README for why install-spark/install-dbt are also required.
install:
	uv sync

# NOTE: install-baseline uses `uv add`, which re-resolves and can mutate pyproject.toml/
# uv.lock. That's fine for adding a genuinely new dependency, but it's the wrong tool for
# clean-clone setup (use `make install` instead) since it can leave a fresh checkout with an
# unexpectedly dirty tree.
install-baseline:
	uv add --dev ruff pytest
	uv add python-dotenv

check:
	uv run ruff check .
	uv run pytest -q

# Installs from the already-declared [project.optional-dependencies] groups via `uv sync`,
# not `uv add` - the groups are fixed, so this should never mutate pyproject.toml/uv.lock.
# `uv sync --extra X` alone defaults to exact mode (it also REMOVES packages from any extra
# not listed), so install-spark and install-dbt each sync BOTH extras together - otherwise
# running one after the other would uninstall the first one's packages. Either target alone
# reproduces the full base+spark+dbt environment.
install-spark:
	uv sync --extra spark --extra dbt

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

# Same reasoning as install-spark above - syncs both extras together so either target alone
# reproduces the full environment. `pyyaml` is already declared in [dependency-groups].dev,
# so plain `uv sync` installs it; no separate `uv add` needed.
install-dbt:
	uv sync --extra spark --extra dbt

smoke-dbt:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt debug --project-dir dbt/qc_lakehouse
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --select hello_dbt

# Isolated venv for Airflow tooling, same reasoning as install-databricks's
# .venv-databricks: Airflow's own dependency constraints are notorious for conflicting with
# an unrelated project's pinned pyspark/dbt-databricks versions, so it never goes in the main
# venv. Airflow itself runs in Docker (see orchestration/docker-compose.yml) - this venv
# exists only so the DAG file can be structurally validated locally via pytest without
# needing Docker running. Pinned to apache-airflow==3.3.1, matching orchestration/
# docker-compose.yml's apache/airflow:3.3.1-python3.12 image tag exactly - same reasoning as
# install-databricks's exact databricks-sdk/databricks-connect pins above: multiple code
# comments, commit messages, and orchestration/tests/test_dag.py assert behavior specific to
# apache-airflow==3.3.1 (e.g. DeadlineReference.DAGRUN_QUEUED_AT semantics), so local
# validation must run against the exact same version the container runs, not whatever
# resolves newest at install time.
install-airflow:
	uv venv .venv-airflow --python 3.12
	uv pip install --python .venv-airflow/bin/python "apache-airflow==3.3.1" apache-airflow-providers-databricks

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
# Guards PII_HASH_SALT being *set but empty* before running: dbt_project.yml's
# `{{ env_var('PII_HASH_SALT') }}` only fails loudly on an *unset* var - an empty string is
# still "set", so `env_var` returns "" silently, --vars passes "" straight through, and
# stg_customers.sql's own fail-fast default gets overridden with an empty salt, producing a
# fully green run with unsalted PII hashes. This is the same failure mode
# scripts/resolve_pii_salt.py's own empty-value guard exists to prevent on the Databricks-Job
# side - this check is the equivalent guard for the local `make dbt-run`/`make dbt-test` path.
dbt-run:
	set -a && . ./.env && set +a && \
	[ -n "$$PII_HASH_SALT" ] || { echo "PII_HASH_SALT is set but empty in .env - refusing to run: this would silently produce unsalted PII hashes in stg_customers." >&2; exit 1; } && \
	DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt run --project-dir dbt/qc_lakehouse --target qc_dev

dbt-test:
	set -a && . ./.env && set +a && \
	[ -n "$$PII_HASH_SALT" ] || { echo "PII_HASH_SALT is set but empty in .env - refusing to run: this would silently test against unsalted PII hashes in stg_customers." >&2; exit 1; } && \
	DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt test --project-dir dbt/qc_lakehouse --target qc_dev

dbt-docs:
	set -a && . ./.env && set +a && DBT_PROFILES_DIR=dbt/qc_lakehouse uv run dbt docs generate --project-dir dbt/qc_lakehouse --target qc_dev

bundle-validate:
	databricks bundle validate

bundle-deploy:
	databricks bundle deploy -t dev
