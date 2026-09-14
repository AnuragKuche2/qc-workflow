from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REQUIRED_KEYS = (
    "DATABRICKS_HOST",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_CATALOG",
    "DATABRICKS_SCHEMA",
)


class MissingSettingsError(RuntimeError):
    def __init__(self, missing_keys: list[str]) -> None:
        self.missing_keys = missing_keys
        super().__init__(
            f"Missing required environment variables: {', '.join(missing_keys)}"
        )


@dataclass(frozen=True)
class Settings:
    databricks_host: str
    databricks_http_path: str
    databricks_catalog: str
    databricks_schema: str


def load_settings(env_file: Path | None = None) -> Settings:
    if env_file is not None:
        load_dotenv(dotenv_path=env_file, override=True)
    else:
        load_dotenv(override=False)

    missing = [key for key in REQUIRED_KEYS if not os.environ.get(key)]
    if missing:
        raise MissingSettingsError(missing)

    return Settings(
        databricks_host=os.environ["DATABRICKS_HOST"],
        databricks_http_path=os.environ["DATABRICKS_HTTP_PATH"],
        databricks_catalog=os.environ["DATABRICKS_CATALOG"],
        databricks_schema=os.environ["DATABRICKS_SCHEMA"],
    )
