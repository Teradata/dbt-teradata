"""Functional tests for IDE-26497: dbt 1.12 behavior-change flags default-on.

dbt Core 1.12 matures several behavior-change flags to enabled-by-default:

  validate_macro_args
    Validates macro call signatures against typed Jinja macro arguments
    (e.g. `{% macro foo(x: string) %}`). No teradata__* macro in this adapter
    uses typed arguments, so this flag is a structural no-op here. Not covered
    by a functional test (there is no adapter behavior to exercise).

  require_all_warnings_handled_by_warn_error (default: true)
    Adapter warnings must be emitted via `exceptions.warn()` (as opposed to
    ad-hoc `log()`/`print()`) so that `--warn-error`/`warn_error_options` can
    promote them to hard failures. dbt-teradata's warnings (persist_docs
    truncation, grants-on-OTF, missing-column) already use `exceptions.warn()`.
    The test below proves one of these warnings (comment truncation) is
    properly promotable to an error under `--warn-error`.

  require_batched_execution_for_custom_microbatch_strategy (default: true)
    Only applies to *custom* `incremental_strategy` macros that implement
    microbatch execution themselves. dbt-teradata's microbatch support is an
    override of dbt-core's built-in `get_incremental_microbatch_sql` macro
    (dispatched through the standard "microbatch" strategy), not a custom
    strategy, so this flag does not change Teradata's microbatch behavior.
    The test below is a regression guard: it re-runs the existing microbatch
    functional scenario with the flag explicitly enabled and confirms
    behavior is unaffected.

  skip_nodes_if_on_run_start_fails (default: true)
    When an on-run-start hook fails, downstream nodes must be skipped
    cleanly rather than partially executed. This is pure dbt-core DAG
    orchestration, but the test below confirms the Teradata adapter's error
    surface (DbtDatabaseError from a failing hook) does not interfere with
    that skip behavior.

  state_modified_compare_more_unrendered_values (default: true)
    Pure dbt-core state-diffing logic with no adapter touchpoint; not
    covered here (would duplicate dbt-core's own test suite with no
    Teradata-specific value).

All tests are functional and require a live Teradata Vantage instance.
All tests in this module are skipped on dbt-core < 1.12 (module-level pytestmark).
"""

import pytest
import dbt.version as _dbt_version
from packaging.version import Version

from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.functional.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
)

_dbt_version_tuple = Version(_dbt_version.__version__).release[:2]

pytestmark = pytest.mark.skipif(
    _dbt_version_tuple < (1, 12),
    reason=f"Requires dbt-core >= 1.12 (found {_dbt_version.__version__})",
)


# ── require_all_warnings_handled_by_warn_error ─────────────────────────

_LONG_DESC = "x" * 400  # exceeds the 255-char Teradata comment limit

_MODEL_SQL = "select 1 as id"

_SCHEMA_LONG_YML = """
version: 2
models:
  - name: long_desc_model
    description: "{long}"
""".replace("{long}", _LONG_DESC)


class TestWarnErrorPromotesPersistDocsWarning:
    """A comment-truncation warning (emitted via exceptions.warn()) must be
    promotable to a hard failure under --warn-error, proving compliance with
    require_all_warnings_handled_by_warn_error.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"long_desc_model.sql": _MODEL_SQL, "schema.yml": _SCHEMA_LONG_YML}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            # Pin the truncation threshold explicitly (rather than relying on
            # the macro's default) so the warning this test depends on stays
            # deterministic even if the default changes elsewhere.
            "vars": {"teradata_max_comment_length": 255},
            "models": {
                "test": {
                    "materialized": "table",
                    "+persist_docs": {"relation": True, "columns": False},
                }
            }
        }

    def test_truncation_warning_succeeds_without_warn_error(self, project):
        results, logs = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert results[0].status == "success"
        assert "exceeds the configured teradata limit" in logs.lower()

    def test_truncation_warning_promoted_with_warn_error(self, project):
        results, logs = run_dbt_and_capture(
            ["--debug", "--warn-error", "run"], expect_pass=False
        )
        assert any(r.status in ("error", "fail") for r in results)
        assert "exceeds the configured teradata limit" in logs.lower()


# ── skip_nodes_if_on_run_start_fails ────────────────────────────────────

_MODEL_A_SQL = "select 1 as id"
_MODEL_B_SQL = "select * from {{ ref('model_a') }}"


class TestSkipNodesIfOnRunStartFails:
    """When an on-run-start hook fails, downstream models must be reported as
    skipped (not run, not partially executed) rather than erroring on the
    Teradata connection.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_a.sql": _MODEL_A_SQL, "model_b.sql": _MODEL_B_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"send_anonymous_usage_stats": False},
            # Deliberate SQL syntax error: fails deterministically regardless
            # of database state (a missing-table query could pass if such a
            # table ever exists in the target schema).
            "on-run-start": ["this is not valid sql at all"],
        }

    def test_downstream_models_are_skipped(self, project):
        results = run_dbt(["run"], expect_pass=False)
        statuses = {getattr(r.node, "name", "on-run-start"): r.status for r in results}

        # The failing hook itself should report an error/fail status.
        assert any(s in ("error", "fail") for s in statuses.values())
        # Both models must be skipped, not executed, since the hook failed first.
        assert statuses.get("model_a") == "skipped"
        assert statuses.get("model_b") == "skipped"


# ── require_batched_execution_for_custom_microbatch_strategy ───────────


class TestMicrobatchWithRequireBatchedExecutionFlag(BaseMicrobatch):
    """Regression guard: dbt-teradata's microbatch override dispatches through
    dbt-core's built-in "microbatch" strategy (not a custom incremental_strategy
    macro), so explicitly enabling require_batched_execution_for_custom_microbatch_strategy
    must not change behavior. Reuses the existing microbatch scenario verbatim.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "send_anonymous_usage_stats": False,
                "require_batched_execution_for_custom_microbatch_strategy": True,
            }
        }
