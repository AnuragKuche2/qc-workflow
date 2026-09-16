# qc-lakehouse/tests/test_dbt_project_files.py
from pathlib import Path

import yaml

DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt" / "qc_lakehouse"


def test_dbt_project_yml_is_valid_and_named_correctly():
    content = yaml.safe_load((DBT_PROJECT_DIR / "dbt_project.yml").read_text())
    assert content["name"] == "qc_lakehouse"
    assert content["profile"] == "qc_lakehouse"


def test_profiles_yml_targets_oauth_auth():
    content = yaml.safe_load((DBT_PROJECT_DIR / "profiles.yml").read_text())
    dev_output = content["qc_lakehouse"]["outputs"]["dev"]
    assert dev_output["type"] == "databricks"
    assert dev_output["auth_type"] == "oauth"
