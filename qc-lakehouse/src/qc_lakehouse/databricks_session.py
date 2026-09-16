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


def build_databricks_session(settings: Settings | None = None):
    """Build a Spark session appropriate to where this process is running.

    When already running ON Databricks (detected via `is_running_on_databricks()` - true
    inside a Databricks Job task on serverless compute), Databricks Connect is unnecessary
    and unusual to use recursively against the same workspace the process is already in -
    build a native Spark session instead via plain `SparkSession.builder.getOrCreate()` and
    return immediately. `settings` is not needed (and may be `None`) on this path: a
    Databricks Job task has no `.env`/environment-variable mechanism to supply it, and there
    is nothing on this path that requires it.

    Locally (e.g. a dev machine or CI), builds a Databricks Connect serverless session for
    the workspace in `settings` - the workspace host is passed explicitly via
    `.host(settings.databricks_host)` so the session always connects to the workspace named
    by `settings`, regardless of what Databricks profile or environment variables happen to
    be ambient. `settings` is required on this path. As a side effect, also ensures
    `settings.databricks_catalog`.`settings.databricks_schema` (Sub-project A's throwaway
    smoke-test schema, typically `workspace.dev`) exists - irrelevant when running as a real
    Databricks Job, which is why that step lives only in this branch.
    """
    if is_running_on_databricks():
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()

    if settings is None:
        raise ValueError(
            "settings is required when not running on Databricks (build_databricks_session "
            "was called off-Databricks with settings=None)"
        )

    from databricks.connect import DatabricksSession

    session = (
        DatabricksSession.builder.host(settings.databricks_host).serverless(True).getOrCreate()
    )
    ensure_schema_exists(session, settings.databricks_catalog, settings.databricks_schema)
    return session
