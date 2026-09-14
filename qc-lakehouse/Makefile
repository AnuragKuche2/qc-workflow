.PHONY: venv install-baseline check

venv:
	uv python pin 3.12
	uv venv

install-baseline:
	uv add --dev ruff pytest
	uv add python-dotenv

check:
	uv run ruff check .
	uv run pytest -q
