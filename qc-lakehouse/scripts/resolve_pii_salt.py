# qc-lakehouse/scripts/resolve_pii_salt.py
"""Reads the `pii_hash_salt` Databricks secret and republishes it as a task value.

Exists only as the `resolve_secrets` prerequisite task in the `dbt_run`/`dbt_test` Databricks
Jobs (see databricks.yml) - dbt's `dbt_task` type has no field for referencing a Databricks
secret directly (checked via `databricks bundle schema`: no such field on `jobs.Task` or
`jobs.DbtTask`), and `{{secrets/scope/key}}` interpolation is not resolved inside `dbt_task`
commands (confirmed live - it reaches dbt's own Jinja renderer unresolved and errors). Task
values, by contrast, *are* documented as substitutable in dbt commands
(`{{tasks.<task_key>.values.<name>}}`), so this task exists purely to bridge
`dbutils.secrets.get` -> a task value the following `dbt_task` can reference in its `--vars`.

Task values only propagate within a single job run's own task DAG, not across separate job
runs - this script (and its `resolve_secrets` task) is duplicated identically in both the
`dbt_run` and `dbt_test` job definitions rather than shared, because each is a separate
top-level job.

Deliberately never prints the secret value itself - only that it was read and published.
That said, this task value is NOT a protected secret end-to-end: the dbt_task that consumes
it passes it through `--vars`, and Databricks' dbt task runner echoes its own resolved shell
command (including the substituted value) into that task's run output/logs in plaintext -
visible to anyone with read/API access to that specific job run (see databricks.yml's
resolve_secrets comments and the task-4 report for the live-confirmed evidence). This is a
known, accepted limitation for a single-user Free Edition workspace, not a fully-protected
secret - don't extend this pattern to a shared/multi-user workspace without re-examining it.
"""
from __future__ import annotations

from databricks.sdk.runtime import dbutils

SCOPE = "qc_lakehouse"
KEY = "pii_hash_salt"


def main() -> None:
    salt = dbutils.secrets.get(scope=SCOPE, key=KEY)
    if not salt:
        # A newline-corrupted secret already broke this once (Databricks' own command-
        # injection guard caught it downstream, in the dbt_task, not here) - an empty value
        # would NOT be caught downstream: --vars would override stg_customers.sql's fail-fast
        # default with "", and the pipeline would silently produce unsalted PII hashes with a
        # fully green run. Fail here instead, where the cause is obvious.
        raise ValueError(
            f"Secret {SCOPE}/{KEY} resolved to an empty value - refusing to publish it as a "
            "task value, since that would silently disable stg_customers.sql's PII salting "
            "instead of failing loudly."
        )
    dbutils.jobs.taskValues.set(key=KEY, value=salt)
    print(f"resolve_pii_salt: OK - read {SCOPE}/{KEY} and published it as a task value")


if __name__ == "__main__":
    main()
