.PHONY: venv install-baseline check install-spark smoke-local

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
