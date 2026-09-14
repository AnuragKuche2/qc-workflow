from qc_lakehouse.databricks_session import ensure_schema_exists


class FakeSession:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def sql(self, query: str) -> None:
        self.queries.append(query)


def test_ensure_schema_exists_issues_create_schema_if_not_exists():
    session = FakeSession()

    ensure_schema_exists(session, catalog="qc_lakehouse", schema="dev")

    assert session.queries == ["CREATE SCHEMA IF NOT EXISTS `qc_lakehouse`.`dev`"]
