"""Functional tests for dbt 1.12 on_error model configuration on Teradata.

dbt Core 1.12 introduces the on_error model config that controls downstream
model behavior when a model fails during a dbt run:

  on_error: skip_children (default)
    Skip all downstream models in the failing model's subtree.

  on_error: continue
    Allow the rest of the DAG to run even if this model fails.

Note: The --fail-fast flag still takes precedence over on_error: continue.

These behaviors are implemented entirely in dbt-core's runner; the adapter is a
pass-through.  These tests confirm the Teradata adapter does not obstruct or
alter the expected semantics.

All tests require a live Teradata Vantage instance and dbt-core >= 1.12.
Tests are skipped automatically on older dbt-core versions.

Teradata Vantage
----------------
Large Teradata warehouse runs often mix mission-critical core tables with
optional enrichment marts.  on_error: continue lets a nightly dbt build
finish loading critical Vantage tables even when an optional model errors,
maximising the value of the batch window and reducing downstream impact.
"""

from dataclasses import dataclass

import dbt.version as _dbt_version
import pytest
from packaging.version import Version

from dbt.tests.util import run_dbt

_dbt_version_tuple = Version(_dbt_version.__version__).release[:2]

pytestmark = pytest.mark.skipif(
    _dbt_version_tuple < (1, 12),
    reason=f"Requires dbt-core >= 1.12 (found {_dbt_version.__version__})",
)

# ── intentionally failing SQL ─────────────────────────────────────────────────
# Each model references a table that does not exist, guaranteeing failure on
# every run regardless of state.

_FAIL_TABLE_DEFAULT_SQL = """\
{{ config(materialized='table') }}
SELECT * FROM dbt_ci_on_error_nonexistent_tbl
"""

_FAIL_TABLE_SKIP_SQL = """\
{{ config(materialized='table', on_error='skip_children') }}
SELECT * FROM dbt_ci_on_error_nonexistent_tbl
"""

_FAIL_TABLE_CONTINUE_SQL = """\
{{ config(materialized='table', on_error='continue') }}
SELECT * FROM dbt_ci_on_error_nonexistent_tbl
"""

_FAIL_VIEW_CONTINUE_SQL = """\
{{ config(materialized='view', on_error='continue') }}
SELECT * FROM dbt_ci_on_error_nonexistent_tbl
"""

_FAIL_INCREMENTAL_CONTINUE_SQL = """\
{{ config(materialized='incremental', on_error='continue') }}
SELECT * FROM dbt_ci_on_error_nonexistent_tbl
"""

_SUCCESS_SQL = """\
{{ config(materialized='table') }}
SELECT 1 AS id
"""


def _child_sql(parent: str) -> str:
    """Return SQL for a model that soft-depends on *parent* but runs on its own.

    The ``-- depends_on`` comment establishes the DAG edge so dbt schedules
    this model after *parent*, but the SELECT 1 body executes independently
    of whether *parent* created a table.  This lets us distinguish between
    a child that was **skipped** (skip_children) and one that **ran and
    succeeded** (continue).
    """
    return (
        f"{{{{ config(materialized='table') }}}}\n"
        f"-- depends_on: {{{{ ref('{parent}') }}}}\n"
        f"SELECT 1 AS id\n"
    )


# ── scenario data ─────────────────────────────────────────────────────────────

@dataclass
class Scenario:
    models: dict
    run_args: list
    expected: dict   # model_name → expected status string
    expect_pass: bool = False


# Special sentinels used in Scenario.expected:
#   None      → model must NOT have status "success" (e.g. --fail-fast case)
#   "absent"  → model must not appear in results at all (e.g. --select case)


# ── shared base class ─────────────────────────────────────────────────────────
# dbt's project_root fixture is class-scoped and keyed by class object, so each
# test class gets its own isolated temp directory.  The base class holds the
# single test method; each concrete subclass supplies a two-line `scenario`.

class _BaseOnErrorTest:
    scenario: Scenario  # set as a class variable by every subclass

    @pytest.fixture(scope="class")
    def models(self):
        return self.__class__.scenario.models

    def test_on_error_behavior(self, project):
        s = self.__class__.scenario
        results = run_dbt(s.run_args, expect_pass=s.expect_pass)
        sm = {r.node.name: r.status for r in results}

        for name, exp in s.expected.items():
            actual = sm.get(name)
            if exp is None:
                assert actual != "success", (
                    f"[{name}] should not succeed when --fail-fast is active; "
                    f"got {actual!r}. Full map: {sm}"
                )
            elif exp == "absent":
                assert name not in sm, (
                    f"[{name}] should be absent from results (not selected); "
                    f"got {actual!r}. Full map: {sm}"
                )
            else:
                assert actual == exp, (
                    f"[{name}] expected {exp!r}, got {actual!r}. Full map: {sm}"
                )


# ── concrete test classes (one per scenario = one isolated dbt project) ───────

class TestOnErrorSkipChildrenDefault(_BaseOnErrorTest):
    """Omitting on_error applies the default skip_children behavior."""
    scenario = Scenario(
        models={
            "model_fail_skip.sql": _FAIL_TABLE_DEFAULT_SQL,
            "model_child_of_skip.sql": _child_sql("model_fail_skip"),
            "model_success_control.sql": _SUCCESS_SQL,
        },
        run_args=["run"],
        expected={
            "model_fail_skip": "error",
            "model_child_of_skip": "skipped",
            "model_success_control": "success",
        },
    )


class TestOnErrorSkipChildrenExplicit(_BaseOnErrorTest):
    """on_error: skip_children set explicitly produces the same result as default."""
    scenario = Scenario(
        models={
            "model_fail_explicit_skip.sql": _FAIL_TABLE_SKIP_SQL,
            "model_child_explicit_skip.sql": _child_sql("model_fail_explicit_skip"),
        },
        run_args=["run"],
        expected={
            "model_fail_explicit_skip": "error",
            "model_child_explicit_skip": "skipped",
        },
    )


class TestOnErrorContinue(_BaseOnErrorTest):
    """on_error: continue lets child models run even when the parent fails."""
    scenario = Scenario(
        models={
            "model_fail_continue.sql": _FAIL_TABLE_CONTINUE_SQL,
            "model_child_of_continue.sql": _child_sql("model_fail_continue"),
            "model_success_ctrl.sql": _SUCCESS_SQL,
        },
        run_args=["run"],
        expected={
            "model_fail_continue": "error",
            "model_child_of_continue": "success",
            "model_success_ctrl": "success",
        },
    )


class TestOnErrorFailFastPrecedence(_BaseOnErrorTest):
    """--fail-fast aborts immediately, overriding on_error: continue."""
    scenario = Scenario(
        models={
            "model_fail_ff.sql": _FAIL_TABLE_CONTINUE_SQL,
            "model_child_ff.sql": _child_sql("model_fail_ff"),
        },
        run_args=["run", "--fail-fast"],
        expected={
            "model_fail_ff": "error",
            "model_child_ff": None,  # must not succeed; --fail-fast overrides continue
        },
    )


class TestOnErrorMultipleModelTypes(_BaseOnErrorTest):
    """on_error: continue applies to table, view, and incremental materializations."""
    scenario = Scenario(
        models={
            "model_table_fail.sql":        _FAIL_TABLE_CONTINUE_SQL,
            "model_view_fail.sql":         _FAIL_VIEW_CONTINUE_SQL,
            "model_incremental_fail.sql":  _FAIL_INCREMENTAL_CONTINUE_SQL,
            "model_child_table.sql":       _child_sql("model_table_fail"),
            "model_child_view.sql":        _child_sql("model_view_fail"),
            "model_child_incremental.sql": _child_sql("model_incremental_fail"),
        },
        run_args=["run"],
        expected={
            "model_table_fail": "error",
            "model_view_fail": "error",
            "model_incremental_fail": "error",
            "model_child_table": "success",
            "model_child_view": "success",
            "model_child_incremental": "success",
        },
    )


class TestOnErrorNestedDAGSkipChildren(_BaseOnErrorTest):
    """skip_children cascades: grandchildren are also skipped when the root fails."""
    scenario = Scenario(
        models={
            "model_root_fail.sql": _FAIL_TABLE_DEFAULT_SQL,
            "model_level1.sql":    _child_sql("model_root_fail"),
            "model_level2.sql":    _child_sql("model_level1"),
        },
        run_args=["run"],
        expected={
            "model_root_fail": "error",
            "model_level1": "skipped",
            "model_level2": "skipped",
        },
    )


class TestOnErrorNestedDAGContinue(_BaseOnErrorTest):
    """With continue, child and grandchild models both run normally."""
    scenario = Scenario(
        models={
            "model_root_cont.sql":   _FAIL_TABLE_CONTINUE_SQL,
            "model_level1_cont.sql": _child_sql("model_root_cont"),
            "model_level2_cont.sql": _child_sql("model_level1_cont"),
        },
        run_args=["run"],
        expected={
            "model_root_cont": "error",
            "model_level1_cont": "success",
            "model_level2_cont": "success",
        },
    )


class TestOnErrorSelectiveRun(_BaseOnErrorTest):
    """on_error: continue works correctly within a selective (--select) run."""
    scenario = Scenario(
        models={
            "model_sel_fail.sql":      _FAIL_TABLE_CONTINUE_SQL,
            "model_sel_child.sql":     _child_sql("model_sel_fail"),
            "model_sel_unrelated.sql": _SUCCESS_SQL,
        },
        run_args=["run", "--select", "model_sel_fail", "model_sel_child"],
        expected={
            "model_sel_fail": "error",
            "model_sel_child": "success",
            "model_sel_unrelated": "absent",  # not selected, must not appear
        },
    )
