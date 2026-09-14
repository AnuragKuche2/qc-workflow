# QC Lakehouse

Foundation & Environment setup is in progress. Full setup instructions land in Task 8
of `docs/superpowers/plans/2026-09-14-qc-lakehouse-foundation-environment.md`.

Design spec: `docs/superpowers/specs/2026-09-14-qc-lakehouse-databricks-hero-design.md`.

## Databricks auth

1. Install the CLI: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
2. `databricks auth login --host https://<your-free-edition-workspace-url>` (opens a browser,
   OAuth U2M - no token to store or rotate).
3. Copy `.env.example` to `.env` and fill in `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`
   (SQL Warehouses -> your warehouse -> Connection details), `DATABRICKS_CATALOG`,
   `DATABRICKS_SCHEMA`.
