from __future__ import annotations

from typing import Protocol

from qc_lakehouse.config import Settings


class SqlRunner(Protocol):
    def sql(self, query: str): ...


def ensure_schema_exists(session: SqlRunner, catalog: str, schema: str) -> None:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def build_databricks_session(settings: Settings):
    """Build a Databricks Connect serverless session for the workspace in `settings`.

    The workspace host is passed explicitly via `.host(settings.databricks_host)` so the
    session always connects to the workspace named by `settings`, regardless of what
    Databricks profile or environment variables happen to be ambient. Do not rely on
    `load_settings()`'s `load_dotenv()` side effect for this - it's no longer load-bearing
    here.
    """
    from databricks.connect import DatabricksSession

    session = (
        DatabricksSession.builder.host(settings.databricks_host).serverless(True).getOrCreate()
    )
    ensure_schema_exists(session, settings.databricks_catalog, settings.databricks_schema)
    return session
