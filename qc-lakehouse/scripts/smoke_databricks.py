"""Smoke test: Databricks serverless compute + Unity Catalog Delta round-trip."""
from __future__ import annotations

from qc_lakehouse.config import load_settings
from qc_lakehouse.databricks_session import build_databricks_session


def main() -> None:
    settings = load_settings()
    spark = build_databricks_session(settings)

    table = (
        f"`{settings.databricks_catalog}`.`{settings.databricks_schema}`"
        ".smoke_databricks_table"
    )

    df = spark.createDataFrame([(1, "ok"), (2, "ok")], ["id", "status"])
    df.write.format("delta").mode("overwrite").saveAsTable(table)

    result = spark.sql(f"SELECT id, status FROM {table} ORDER BY id")
    rows = {row["id"]: row["status"] for row in result.collect()}

    assert rows == {1: "ok", 2: "ok"}, f"unexpected rows: {rows}"
    print(
        f"smoke-databricks: OK - wrote and read back 2 rows via {table} on serverless compute"
    )


if __name__ == "__main__":
    main()
