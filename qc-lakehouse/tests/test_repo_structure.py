from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

REQUIRED_PATHS = [
    "conf",
    "src/qc_lakehouse",
    "dbt",
    "scripts",
    "tests",
    ".env.example",
    ".gitignore",
    "README.md",
]


def test_required_top_level_paths_exist():
    missing = [p for p in REQUIRED_PATHS if not (REPO_ROOT / p).exists()]
    assert missing == [], f"missing required paths: {missing}"
