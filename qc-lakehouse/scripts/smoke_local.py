# qc-lakehouse/scripts/smoke_local.py
"""Smoke test: local Spark + Delta round-trip. Requires a real JDK and starts a JVM, so it's
a separate Makefile target rather than a pytest test."""
from __future__ import annotations

import shutil
from pathlib import Path

from qc_lakehouse.spark_local import build_local_spark_session

WAREHOUSE_DIR = Path("warehouse")
TABLE_PATH = str(WAREHOUSE_DIR / "smoke_local_table")


def main() -> None:
    if WAREHOUSE_DIR.exists():
        shutil.rmtree(WAREHOUSE_DIR)

    spark = build_local_spark_session()
    try:
        df = spark.createDataFrame([(1, "ok"), (2, "ok")], ["id", "status"])
        df.write.format("delta").mode("overwrite").save(TABLE_PATH)

        result = spark.read.format("delta").load(TABLE_PATH)
        rows = {row["id"]: row["status"] for row in result.collect()}

        assert rows == {1: "ok", 2: "ok"}, f"unexpected rows: {rows}"
        print("smoke-local: OK - wrote and read back 2 rows via local Delta table")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
