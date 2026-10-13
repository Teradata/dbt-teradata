import pytest

from dbt.tests.adapter.store_test_failures_tests import fixtures
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    BaseStoreTestFailures,
    BaseStoreTestFailuresLimit,
)

# Teradata does not support boolean literal 'false'; use 1=0 instead.
_PASSING_TEST_TD = """
select * from {{ ref('fine_model') }}
where 1=0
"""


# The raw-drop helper and its "database does not exist" error code live in
# tests/teradata_cleanup.py so the error code is defined once for the whole
# suite (conftest.py and test_basic.py use the same helper).
from tests.teradata_cleanup import run_sql_ignoring_missing_database


class _TeradataAuditSchemaCleanupMixin:
    # BaseStoreTestFailures/BaseStoreTestFailuresLimit's own `clean_up` fixture
    # drops the test audit schema (a *different* schema from the primary test
    # schema, e.g. "<test_schema>_dbt_test__aud") via `adapter.drop_schema()`.
    # That macro calls `adapter.verify_database()`, a safety guard that
    # intentionally rejects DROP/DELETE DATABASE against any database other
    # than the connection's configured one -- exactly what's happening here,
    # since the audit schema is a second, differently-named database. Rather
    # than weaken that guard, drop the audit schema directly via raw SQL,
    # bypassing the guarded macro for this one intentional cross-schema case.
    @pytest.fixture(scope="function")
    def clean_up(self, project):
        yield
        # Only the "database does not exist" error is expected/benign here
        # (the audit schema may not have been created, e.g. if the test
        # failed before storing any test failures); any other error is a
        # real failure and must propagate instead of being silently
        # swallowed, so the suite doesn't leak the audit schema unnoticed.
        run_sql_ignoring_missing_database(
            project, f"DELETE DATABASE {self.test_audit_schema} ALL;"
        )
        run_sql_ignoring_missing_database(
            project, f"DROP DATABASE {self.test_audit_schema};"
        )


class TestStoreTestFailuresTeradata(_TeradataAuditSchemaCleanupMixin, BaseStoreTestFailures):
    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "failing_test.sql": fixtures.tests__failing_test,
            "passing_test.sql": _PASSING_TEST_TD,
        }


class TestStoreTestFailuresLimitTeradata(_TeradataAuditSchemaCleanupMixin, BaseStoreTestFailuresLimit):
    # No `tests` override is needed here, unlike TestStoreTestFailuresTeradata
    # above. BaseStoreTestFailuresLimit replaces the inherited `tests` fixture
    # with an empty dict (it also empties `models` and drives its assertions
    # purely from the seeded people.csv plus properties__seeds_yml), so the
    # upstream passing_test fixture -- and its PostgreSQL-only `where false`
    # predicate -- is never rendered for this class.
    pass
