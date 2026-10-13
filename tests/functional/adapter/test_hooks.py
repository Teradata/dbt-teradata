import os
from pathlib import Path

import pytest

from dbt.tests.adapter.hooks.test_model_hooks import (
    MODEL_PRE_HOOK,
    MODEL_POST_HOOK,
    BasePrePostModelHooks,
    BaseHookRefs,
    BasePrePostModelHooksOnSeeds,
    BasePrePostModelHooksOnSeedsPlusPrefixed,
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace,
    BasePrePostModelHooksOnSnapshots,
    BasePrePostModelHooksInConfig,
    BasePrePostModelHooksInConfigWithCount,
    BasePrePostModelHooksInConfigKwargs,
    BasePrePostSnapshotHooksInConfigKwargs,
    BaseDuplicateHooksInConfigs,
    BaseHooksRefsOnSeeds,
    BaseTestPrePost,
)
from dbt.tests.adapter.hooks.test_run_hooks import (
    BasePrePostRunHooks,
    BaseAfterRunHooks,
)

# Teradata ADD COLUMN syntax: no COLUMN keyword, no parentheses for a single column.
# The parenthesized form `ADD (col type)` requires the target table to already be
# column-partitioned (Error 9478: "Table must be column partitioned"), which these
# plain seed/snapshot tables are not.
# Teradata does not support boolean literals; use CAST(NULL AS INT) instead of null::int
_TD_SEEDS_POST_HOOK = [
    "alter table {{ this }} add new_col int",
    "update {{ this }} set new_col = 1",
    "select cast(null as {{ dbt.type_int() }}) as id",
]
_TD_SNAPSHOT_POST_HOOK = [
    "alter table {{ this }} add new_col int",
    "update {{ this }} set new_col = 1",
]

# The upstream test suite runs a Postgres-specific `vacuum <table>` statement as the
# out-of-transaction half of the pre/post hook pair. `VACUUM` is not a Teradata
# statement (Error 3706), so we substitute a harmless no-op query that still exercises
# the "transaction: False" hook path.
_TD_NOOP_HOOK = {"sql": "select 1", "transaction": False}

# The base `check_hooks`/`check_hooks` implementations in dbt-tests-adapter hardcode
# Postgres-specific expectations (target_type == "postgres", target_user == "root",
# target_dbname == "dbt", target_pass == ""). Teradata's actual values differ, so we
# override the assertions to reflect what the Teradata adapter/profile actually expose,
# verified live: target_type == "teradata", target_user == configured username,
# target_dbname == "" (no `database` key set in the test profile), target_pass == ""
# (the profile has no Jinja `pass` alias for Teradata, only `password`).
_TD_PROFILE_USER = os.getenv("DBT_TERADATA_USERNAME", "dbc")

# The test profile sets no `database` key, so the credentials' `database` field
# (which `target.dbname` aliases) is Python None and renders as the empty
# string. Asserted rather than merely described so the value is pinned by the
# suite: upstream asserts target_dbname == "dbt", and simply dropping the
# assertion when overriding check_hooks would silently lose coverage of a
# context variable the base suite deliberately checks.
_TD_TARGET_DBNAME = ""


def _assert_hook_ctx(ctx, state, project, host):
    """Assert one hook context against the Teradata profile's actual values.

    Shared by both `check_hooks` overrides below. They cannot share a single
    `check_hooks` because their upstream base classes call it with different
    signatures (`BasePrePostModelHooks` passes a `count`; the run-hooks suite
    fetches a single ctx), but the assertion body is identical -- keeping it
    here means a newly asserted context variable only has to be added once.
    """
    assert ctx["test_state"] == state
    assert ctx["target_host"] == host
    assert ctx["target_name"] == "default"
    assert ctx["target_schema"] == project.test_schema
    assert ctx["target_threads"] == 4
    assert ctx["target_type"] == "teradata"
    assert ctx["target_user"] == _TD_PROFILE_USER
    assert ctx["target_pass"] == ""
    assert ctx["target_dbname"] == _TD_TARGET_DBNAME

    assert (
        ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
    ), "run_started_at was not set"
    assert (
        ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
    ), "invocation_id was not set"
    assert ctx["thread_id"].startswith("Thread-") or ctx["thread_id"] == "MainThread"


class _TeradataCheckHooksMixin:
    def check_hooks(self, state, project, host, count=1):
        ctxs = self.get_ctx_vars(state, count=count, project=project)
        for ctx in ctxs:
            _assert_hook_ctx(ctx, state, project, host)


class TestPrePostModelHooksTeradata(_TeradataCheckHooksMixin, BasePrePostModelHooks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [MODEL_PRE_HOOK, _TD_NOOP_HOOK],
                    "post-hook": [_TD_NOOP_HOOK, MODEL_POST_HOOK],
                }
            }
        }


class TestHookRefsTeradata(_TeradataCheckHooksMixin, BaseHookRefs):
    pass


class TestPrePostModelHooksOnSeedsTeradata(BasePrePostModelHooksOnSeeds):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "post-hook": _TD_SEEDS_POST_HOOK,
                "quote_columns": False,
            },
        }


class TestHooksRefsOnSeedsTeradata(BaseHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedTeradata(BasePrePostModelHooksOnSeedsPlusPrefixed):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": _TD_SEEDS_POST_HOOK[:2],
                "quote_columns": False,
            },
        }


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespaceTeradata(BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": _TD_SEEDS_POST_HOOK[:2],
                "quote_columns": False,
            },
        }


class TestPrePostModelHooksOnSnapshotsTeradata(BasePrePostModelHooksOnSnapshots):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "snapshot-paths": ["test-snapshots"],
            "models": {},
            "snapshots": {"post-hook": _TD_SNAPSHOT_POST_HOOK},
            "seeds": {"quote_columns": False},
        }


class TestPrePostModelHooksInConfigTeradata(_TeradataCheckHooksMixin, BasePrePostModelHooksInConfig):
    pass


class TestPrePostModelHooksInConfigWithCountTeradata(
    _TeradataCheckHooksMixin, BasePrePostModelHooksInConfigWithCount
):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [MODEL_PRE_HOOK, _TD_NOOP_HOOK],
                    "post-hook": [_TD_NOOP_HOOK, MODEL_POST_HOOK],
                }
            }
        }


class TestPrePostModelHooksInConfigKwargsTeradata(_TeradataCheckHooksMixin, BasePrePostModelHooksInConfigKwargs):
    pass


class TestPrePostSnapshotHooksInConfigKwargsTeradata(BasePrePostSnapshotHooksInConfigKwargs):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "snapshot-paths": ["test-kwargs-snapshots"],
            "models": {},
            "snapshots": {"post-hook": _TD_SNAPSHOT_POST_HOOK},
            "seeds": {"quote_columns": False},
        }


class TestDuplicateHooksInConfigsTeradata(BaseDuplicateHooksInConfigs):
    pass


class TestPrePostRunHooksTeradata(BasePrePostRunHooks):
    # The base fixture uses `drop table if exists ...`, which is not valid Teradata
    # syntax (Error 3707); Teradata's IF-EXISTS suppression only works through the
    # adapter's own add_query() hint handling, not raw test-fixture SQL. Guard each
    # drop instead, ignoring "does not exist" (Error 3807).
    @pytest.fixture(scope="function")
    def setUp(self, project):
        # This fixture is function-scoped (runs before every test in the class), so
        # unlike the class-scoped hooks setUp, these tables can already exist from a
        # prior test in the same class and must be dropped first.
        for table in ("on_run_hook", "schemas", "db_schemas"):
            try:
                project.run_sql(f"drop table {project.test_schema}.{table}")
            except Exception as exc:
                if "3807" not in str(exc):
                    raise
        project.run_sql_file(project.test_data_dir / Path("seed_run.sql"))
        os.environ["TERM_TEST"] = "TESTING"

    # Teradata has no `text` type; use varchar instead.
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "on-run-start": [
                "{{ custom_run_hook('start', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.start_hook_order_test ( id int )",
                "drop table {{ target.schema }}.start_hook_order_test",
                "{{ log(env_var('TERM_TEST'), info=True) }}",
            ],
            "on-run-end": [
                "{{ custom_run_hook('end', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.end_hook_order_test ( id int )",
                "drop table {{ target.schema }}.end_hook_order_test",
                "create table {{ target.schema }}.schemas ( schema varchar(128) )",
                "insert into {{ target.schema }}.schemas (schema) values {% for schema in schemas %}( '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
                "create table {{ target.schema }}.db_schemas ( db varchar(128), schema varchar(128) )",
                "insert into {{ target.schema }}.db_schemas (db, schema) values {% for db, schema in database_schemas %}('{{ db }}', '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
            ],
            "seeds": {
                "quote_columns": False,
            },
        }

    def check_hooks(self, state, project, host):
        _assert_hook_ctx(self.get_ctx_vars(state, project), state, project, host)

    # The test profile has no `database` set, so `project.database` (== the
    # credentials' `database` field) is Python `None`, while the Jinja-rendered
    # `database_schemas` value inserted into the db_schemas table stringifies that
    # same `None` to the literal text "None". Compare against the stringified form.
    def assert_used_schemas(self, project):
        schemas_query = "select * from {}.schemas".format(project.test_schema)
        results = project.run_sql(schemas_query, fetch="all")
        assert len(results) == 1
        assert results[0][0] == project.test_schema

        db_schemas_query = "select * from {}.db_schemas".format(project.test_schema)
        results = project.run_sql(db_schemas_query, fetch="all")
        assert len(results) == 1
        assert results[0][0] == str(project.database)
        assert results[0][1] == project.test_schema


class TestAfterRunHooksTeradata(BaseAfterRunHooks):
    pass


class TestPrePostTeradata(BaseTestPrePost):
    pass
