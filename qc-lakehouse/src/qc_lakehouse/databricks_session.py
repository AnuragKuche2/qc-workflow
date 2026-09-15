from __future__ import annotations

import os
from typing import Protocol

from qc_lakehouse.config import Settings


class SqlRunner(Protocol):
    def sql(self, query: str): ...


def ensure_schema_exists(session: SqlRunner, catalog: str, schema: str) -> None:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def is_running_on_databricks() -> bool:
    """True when this process is already executing on Databricks (e.g. as a Databricks Job
    task), detected via DATABRICKS_RUNTIME_VERSION - a variable Databricks sets automatically
    on every job/cluster runtime and that never exists on a local machine."""
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def build_databricks_session(settings: Settings):
    """Build a Spark session appropriate to where this process is running.

    Locally (e.g. a dev machine or CI), builds a Databricks Connect serverless session for
    the workspace in `settings` - the workspace host is passed explicitly via
    `.host(settings.databricks_host)` so the session always connects to the workspace named
    by `settings`, regardless of what Databricks profile or environment variables happen to
    be ambient.

    When already running ON Databricks (detected via `is_running_on_databricks()` - true
    inside a Databricks Job task on serverless compute), Databricks Connect is unnecessary
    and unusual to use recursively against the same workspace the process is already in -
    build a native Spark session instead via plain `SparkSession.builder.getOrCreate()`.
    """
    if is_running_on_databricks():
        from pyspark.sql import SparkSession

        session = SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession

        session = (
            DatabricksSession.builder.host(settings.databricks_host).serverless(True).getOrCreate()
        )

    ensure_schema_exists(session, settings.databricks_catalog, settings.databricks_schema)
    return session
