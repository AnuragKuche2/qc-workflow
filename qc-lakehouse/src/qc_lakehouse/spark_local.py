# qc-lakehouse/src/qc_lakehouse/spark_local.py
from __future__ import annotations

from pathlib import Path

DEFAULT_CONF_PATH = Path(__file__).resolve().parents[2] / "conf" / "spark-local.conf"


def parse_spark_conf(conf_path: Path) -> dict[str, str]:
    conf: dict[str, str] = {}
    for raw_line in conf_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        key, value = parts
        conf[key] = value.strip()
    return conf


def build_local_spark_session(conf_path: Path = DEFAULT_CONF_PATH):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    conf = parse_spark_conf(conf_path)
    builder = SparkSession.builder.appName("qc-lakehouse-local")
    for key, value in conf.items():
        builder = builder.config(key, value)

    return configure_spark_with_delta_pip(builder).getOrCreate()
