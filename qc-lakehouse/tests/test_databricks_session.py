from qc_lakehouse.databricks_session import ensure_schema_exists, is_running_on_databricks


class FakeSession:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def sql(self, query: str) -> None:
        self.queries.append(query)


def test_ensure_schema_exists_issues_create_schema_if_not_exists():
    session = FakeSession()

    ensure_schema_exists(session, catalog="qc_lakehouse", schema="dev")

    assert session.queries == ["CREATE SCHEMA IF NOT EXISTS `qc_lakehouse`.`dev`"]


def test_is_running_on_databricks_false_when_env_var_unset(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert is_running_on_databricks() is False


def test_is_running_on_databricks_true_when_env_var_set(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    assert is_running_on_databricks() is True
