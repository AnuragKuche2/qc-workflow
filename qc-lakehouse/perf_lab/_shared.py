"""Shared constants for Sub-project H's perf_lab scripts."""
from __future__ import annotations

import os

# This project's single Databricks Free Edition SQL warehouse. Overridable via
# DATABRICKS_WAREHOUSE_ID for a different workspace - see .env.example. The literal default is
# this project's own warehouse (Free Edition provisions exactly one, non-resizable, per
# workspace).
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "ca865a4ef1668613")
