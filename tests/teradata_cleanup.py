"""Shared raw-SQL schema cleanup helpers for the Teradata test suite.

Teradata equates a database with a schema, so `teradata__drop_schema` guards
every drop with `adapter.verify_database()` and refuses to drop anything other
than the database configured in the profile. Several upstream base tests create
additional schemas (alternate doc-generation schemas, the `_dbt_test__aud`
audit schema, clone targets) and expect teardown to drop them, which trips that
guard.

The suite's answer is to drop those extra schemas directly via raw SQL -- an
explicit, intentional exception to the guard, confined to test teardown. This
module is the single home for that pattern so the error code below is defined
once instead of being copied into every module that needs it.
"""

# Teradata error code raised by DELETE/DROP DATABASE when the target database
# does not exist -- the only failure this cleanup is allowed to swallow. It
# mirrors the `/*+ IF EXISTS */` suppression list in
# TeradataConnectionManager.add_query, which `project.run_sql()` bypasses since
# it executes via a raw cursor (see run_sql_for_tests).
DATABASE_DOES_NOT_EXIST_ERROR = "[Error 3802]"


def run_sql_ignoring_missing_database(project, sql):
    """Run `sql` via `project.run_sql`, swallowing only "database not found".

    Any other exception -- including the KeyError/AttributeError raised when no
    adapter is registered -- propagates to the caller, so a schema that is
    genuinely being leaked on the server still fails the test rather than
    passing silently.
    """
    try:
        project.run_sql(sql)
    except Exception as exc:
        if DATABASE_DOES_NOT_EXIST_ERROR not in str(exc):
            raise
