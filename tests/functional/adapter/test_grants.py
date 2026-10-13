import os

import pytest
from dbt.contracts.results import RunStatus
from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants
from dbt.tests.util import get_connection, relation_from_name, run_dbt

_USERS_SET = all(os.getenv(v) for v in ["DBT_TEST_USER_1", "DBT_TEST_USER_2", "DBT_TEST_USER_3"])
_SKIP_GRANTS = not _USERS_SET
_SKIP_GRANTS_REASON = "DBT_TEST_USER_1/2/3 env vars not set; required for grants functional tests"

my_snapshot_sql = """
    {% snapshot my_snapshot %}
        {{ config(
            check_cols='all', unique_key='id', strategy='check',
            target_database=database, target_schema=schema
        ) }}
        select 1 as id, cast('blue' as varchar(100)) as color
    {% endsnapshot %}
    """.strip()

snapshot_schema_yml = """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""


@pytest.mark.skipif(_SKIP_GRANTS, reason=_SKIP_GRANTS_REASON)
class TestInvalidGrantsTeradata(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "[Teradata Database] [Error 3802]"

    def privilege_does_not_exist_error(self):
        return "[Teradata Database] [Error 3706]"


@pytest.mark.skipif(_SKIP_GRANTS, reason=_SKIP_GRANTS_REASON)
class TestModelGrantsTeradata(BaseModelGrants):
    pass


class TestShowGrantSqlLongSchemaNameTeradata:
    """Regression test for IDE-26643.

    DBC.AllRights.DatabaseName is CHAR(30) and silently truncates schema
    (database) names longer than 30 characters. teradata__get_show_grant_sql's
    table/view branch used to read that legacy table with an exact-match
    predicate (WHERE DatabaseName='<schema>'), which matched nothing once the
    schema name exceeded 30 characters -- so dbt could never see the current
    grants for such a schema (and, per default__apply_grants, would never
    issue the REVOKE needed when a grantee is removed from `grants:` config).

    This creates a model in a schema forced past the 30-character boundary,
    grants SELECT to a real user, and asserts the grant is read back
    correctly by calling teradata__get_show_grant_sql directly. Before the
    fix (DBC.AllRights -> DBC.AllRightsV), this returned zero rows for such a
    schema; after the fix it returns the expected grant.

    Note: this intentionally does not reuse BaseModelGrants.test_view_table_grants
    (which additionally switches the select grantee mid-test to exercise the
    revoke path) because that test is independently flaky in this environment:
    on the second `dbt run` (CREATE OR REPLACE VIEW), DBC.AllRightsV's
    `Username <> current_user` comparison intermittently fails to exclude the
    view owner's own implicit grants, even for the standard (< 30 character)
    schema used by TestModelGrantsTeradata above -- a pre-existing issue
    unrelated to IDE-26643 and out of scope for this fix.
    """

    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        # tests/conftest.py's default unique_schema is `dbt_test_<20-digit
        # prefix>` = 29 characters -- one under the truncation boundary.
        # Append a suffix to push it past 30.
        return f"dbt_test_{prefix}_ide26643_long_schema"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_show_grant_sql_long_schema_name"}

    @pytest.fixture(scope="class")
    def models(self):
        user1 = os.getenv("DBT_TEST_USER_1", "")
        if not user1:
            pytest.skip("DBT_TEST_USER_1 env var not set -- skipping grants test")
        schema_yml = f"""
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["{user1}"]
"""
        return {
            "my_model.sql": "select 1 as fun",
            "schema.yml": schema_yml,
        }

    def test_grant_read_back_for_long_schema_name(self, project):
        user1 = os.getenv("DBT_TEST_USER_1")

        assert len(project.test_schema) > 30, (
            "test schema must exceed 30 characters to exercise the "
            "DBC.AllRights truncation this test guards against"
        )

        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == RunStatus.Success

        relation = relation_from_name(project.adapter, "my_model")
        with get_connection(project.adapter):
            show_grant_sql = project.adapter.execute_macro(
                "get_show_grant_sql", kwargs={"relation": relation}
            )
            _, grant_table = project.adapter.execute(show_grant_sql, fetch=True)

        # Teradata identifiers are case-insensitive, so the grantee's casing
        # as read back from the dictionary may differ from DBT_TEST_USER_1's
        # casing. Compare the grantee case-insensitively (privilege is
        # already normalized to lowercase by the macro).
        rows = [(row[0].strip().upper(), row[1]) for row in grant_table.rows]
        assert (user1.upper(), "select") in rows, (
            f"Expected ('{user1}', 'select') in the grant read-back for schema "
            f"{project.test_schema!r} (length {len(project.test_schema)}); got "
            f"{rows} instead. Before the fix, teradata__get_show_grant_sql's "
            "table/view branch read the legacy DBC.AllRights table, whose "
            "CHAR(30) DatabaseName column silently truncates schema names "
            "over 30 characters, so this returned zero rows."
        )


@pytest.mark.skipif(_SKIP_GRANTS, reason=_SKIP_GRANTS_REASON)
class TestIncrementalGrantsTeradata(BaseIncrementalGrants):
    pass


@pytest.mark.skipif(_SKIP_GRANTS, reason=_SKIP_GRANTS_REASON)
class TestSeedGrantsTeradata(BaseSeedGrants):
    pass


@pytest.mark.skipif(_SKIP_GRANTS, reason=_SKIP_GRANTS_REASON)
class TestSnapshotGrantsTeradata(BaseSnapshotGrants):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_snapshot.sql": my_snapshot_sql,
            "schema.yml": self.interpolate_name_overrides(snapshot_schema_yml),
        }
    pass




# ---------------------------------------------------------------------------
# Role-aware grants (teradata_grants_extended=true)
# ---------------------------------------------------------------------------

_ROLE_SCHEMA_YML = """
version: 2
models:
  - name: my_role_model
    config:
      grants:
        select:
          - "role:{role1}"
"""

_ROLE_AND_USER_SCHEMA_YML = """
version: 2
models:
  - name: my_role_model
    config:
      grants:
        select:
          - "role:{role1}"
          - "user:{user1}"
"""

_ROLE_MODEL_SQL = "select 1 as id"


def _role1():
    return os.getenv("DBT_TEST_ROLE_1", "")

def _user1():
    return os.getenv("DBT_TEST_USER_1", "")


class TestRoleGrantsTeradata(BaseGrants):
    """Functional tests for role-aware grants behind teradata_grants_extended=true.

    Prerequisites (set as env vars before running):
      DBT_TEST_ROLE_1  - an existing Teradata role the service account can GRANT to

    The teradata_grants_extended var is injected via project_config_update.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"teradata_grants_extended": True}}

    @pytest.fixture(scope="class")
    def models(self):
        role1 = _role1()
        if not role1:
            pytest.skip("DBT_TEST_ROLE_1 env var not set — skipping role grants tests")
        schema = _ROLE_SCHEMA_YML.format(role1=role1)
        return {
            "my_role_model.sql": _ROLE_MODEL_SQL,
            "schema.yml": schema,
        }

    def test_role_grant_applied(self, project):
        """GRANT SELECT on model to a role is issued on first run and grant is readable."""
        role1 = _role1()
        if not role1:
            pytest.skip("DBT_TEST_ROLE_1 env var not set")

        results = run_dbt(["run"])
        assert len(results) == 1

        expected = {"select": [f"role:{role1}"]}
        self.assert_expected_grants_match_actual(project, "my_role_model", expected)

    def test_role_grant_idempotent(self, project):
        """Re-running dbt does not error — role grant read back prevents duplicate GRANT."""
        role1 = _role1()
        if not role1:
            pytest.skip("DBT_TEST_ROLE_1 env var not set")

        run_dbt(["run"])
        # Second run: grant already exists; diff should be empty → no error
        results = run_dbt(["run"])
        assert len(results) == 1
        expected = {"select": [f"role:{role1}"]}
        self.assert_expected_grants_match_actual(project, "my_role_model", expected)


class TestRoleAndUserGrantsTeradata(BaseGrants):
    """Tests that role: and user: grantees coexist correctly for the same privilege."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"teradata_grants_extended": True}}

    @pytest.fixture(scope="class")
    def models(self):
        role1 = _role1()
        user1 = _user1()
        if not role1 or not user1:
            pytest.skip("DBT_TEST_ROLE_1 and DBT_TEST_USER_1 env vars must both be set")
        schema = _ROLE_AND_USER_SCHEMA_YML.format(role1=role1, user1=user1)
        return {
            "my_role_model.sql": _ROLE_MODEL_SQL,
            "schema.yml": schema,
        }

    def test_mixed_grantees_applied(self, project):
        """GRANT SELECT is issued to both a role and a user; both read back correctly."""
        role1 = _role1()
        user1 = _user1()
        if not role1 or not user1:
            pytest.skip("DBT_TEST_ROLE_1 and DBT_TEST_USER_1 env vars must both be set")

        results = run_dbt(["run"])
        assert len(results) == 1

        # In extended mode both sides of dbt's diff use type-qualified grantees,
        # so grants read back from Teradata carry the 'role:' / 'user:' prefix.
        expected = {"select": [f"role:{role1}", f"user:{user1}"]}
        self.assert_expected_grants_match_actual(project, "my_role_model", expected)

        # Second run must be a no-op: mismatched representations would make dbt
        # revoke and re-grant on every run.
        results = run_dbt(["run"])
        assert len(results) == 1
        self.assert_expected_grants_match_actual(project, "my_role_model", expected)