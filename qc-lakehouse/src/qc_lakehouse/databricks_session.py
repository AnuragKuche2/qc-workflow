from __future__ import annotations

from typing import Protocol

from qc_lakehouse.config import Settings


class SqlRunner(Protocol):
    def sql(self, query: str): ...


def ensure_schema_exists(session: SqlRunner, catalog: str, schema: str) -> None:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def build_databricks_session(settings: Settings):
    from databricks.connect import DatabricksSession

    session = DatabricksSession.builder.serverless(True).getOrCreate()
    ensure_schema_exists(session, settings.databricks_catalog, settings.databricks_schema)
    return session
