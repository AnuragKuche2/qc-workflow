import pytest

from qc_lakehouse.config import MissingSettingsError, load_settings

REQUIRED_KEYS = (
    "DATABRICKS_HOST",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_CATALOG",
    "DATABRICKS_SCHEMA",
)


def test_load_settings_reads_all_required_keys(tmp_path, monkeypatch):
    for key in REQUIRED_KEYS:
        monkeypatch.delenv(key, raising=False)

    env_file = tmp_path / ".env"
    env_file.write_text(
        "DATABRICKS_HOST=adb-123.cloud.databricks.com\n"
        "DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123\n"
        "DATABRICKS_CATALOG=qc_lakehouse\n"
        "DATABRICKS_SCHEMA=dev\n"
    )

    settings = load_settings(env_file=env_file)

    assert settings.databricks_host == "adb-123.cloud.databricks.com"
    assert settings.databricks_http_path == "/sql/1.0/warehouses/abc123"
    assert settings.databricks_catalog == "qc_lakehouse"
    assert settings.databricks_schema == "dev"


def test_load_settings_raises_on_missing_keys(tmp_path, monkeypatch):
    for key in REQUIRED_KEYS:
        monkeypatch.delenv(key, raising=False)

    env_file = tmp_path / ".env"
    env_file.write_text("DATABRICKS_HOST=adb-123.cloud.databricks.com\n")

    with pytest.raises(MissingSettingsError) as exc_info:
        load_settings(env_file=env_file)

    assert "DATABRICKS_HTTP_PATH" in str(exc_info.value)
    assert "DATABRICKS_CATALOG" in str(exc_info.value)
    assert "DATABRICKS_SCHEMA" in str(exc_info.value)
    assert exc_info.value.missing_keys == [
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_CATALOG",
        "DATABRICKS_SCHEMA",
    ]
