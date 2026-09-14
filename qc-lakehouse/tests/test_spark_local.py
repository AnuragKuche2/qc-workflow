# qc-lakehouse/tests/test_spark_local.py
from qc_lakehouse.spark_local import parse_spark_conf


def test_parse_spark_conf_reads_key_value_pairs(tmp_path):
    conf_file = tmp_path / "spark-local.conf"
    conf_file.write_text(
        "# comment line, ignored\n"
        "\n"
        "spark.sql.warehouse.dir       ./warehouse\n"
        "spark.sql.shuffle.partitions  4\n"
    )

    conf = parse_spark_conf(conf_file)

    assert conf == {
        "spark.sql.warehouse.dir": "./warehouse",
        "spark.sql.shuffle.partitions": "4",
    }
