"""Functional tests for IDE-26492: Certify vars.yml support on dbt-teradata.

dbt 1.12 introduces a dedicated vars.yml file at the project root. This file
is loaded with plain YAML (no Jinja rendering) before dbt_project.yml, making
all declared variables available when dbt renders dbt_project.yml with Jinja.

All tests require a live Teradata Vantage instance and are skipped on
dbt-core < 1.12.

Coverage (mapped to Jira IDE-26492 acceptance criteria):
  AC-1  vars.yml is parsed correctly
  AC-2  variables in vars.yml are accessible in dbt_project.yml model config
  AC-3  variable interpolation in dbt_project.yml models section
  AC-4  vars.yml variables available in model Jinja context (env_var, target)
  AC-5  cannot define vars in both vars.yml and dbt_project.yml simultaneously
  AC-6  temporary_metadata_generation_schema via vars.yml
  AC-7  query_band configuration variables via vars.yml
  AC-8  table_kind and other Teradata-specific defaults via vars.yml
  AC-9  environment-specific variable overrides
  AC-10 variable references in models (via var())
  AC-11 CI environment variable injection into vars.yml

vars.yml format note
--------------------
dbt reads vars.yml with ``vars_file_dict.get("vars", {})``, so the file MUST
have a top-level ``vars:`` key:

    vars:
      my_var: value

Plain top-level keys (without the ``vars:`` wrapper) are silently ignored.

Mutual exclusivity note
-----------------------
dbt 1.12 raises DbtProjectError if BOTH vars.yml and dbt_project.yml's
``vars:`` section contain any variables.  Test classes that set vars via
vars.yml must NOT have a ``vars:`` key in their ``project_config_update``.

Implementation note
-------------------
Variables in vars.yml are loaded by RuntimeConfig.from_args() during the
``adapter`` fixture setup.  The correct injection point is therefore the
``vars_yml_update`` fixture provided by dbt.tests.fixtures.project — it feeds
into the ``vars_yml`` fixture, which writes vars.yml *before* the adapter
fixture runs.
"""

import os
import pathlib
import re

import pytest
import dbt.version as _dbt_version
from packaging.version import Version
from dbt.exceptions import DbtProjectError

from dbt.tests.util import read_file, run_dbt, run_dbt_and_capture

_dbt_version_tuple = Version(_dbt_version.__version__).release[:2]

pytestmark = pytest.mark.skipif(
    _dbt_version_tuple < (1, 12),
    reason=f"Requires dbt-core >= 1.12 (found {_dbt_version.__version__})",
)


# ---------------------------------------------------------------------------
# AC-1, AC-10: Basic vars.yml parsing — vars accessible in models via var()
# ---------------------------------------------------------------------------

_BASIC_MODEL_SQL = """\
select
    '{{ var("work_schema") }}' as work_schema,
    '{{ var("app_label") }}'   as app_label
"""


class TestVarsYmlBasicParsing:
    """AC-1, AC-10: vars.yml is parsed and variables are accessible in models."""

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        return {
            "vars": {
                "work_schema": "dbt_test_work",
                "app_label": "dbt_test_app",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"vars_basic.sql": _BASIC_MODEL_SQL}

    def test_vars_yml_variables_accessible_in_models(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        row = project.run_sql(
            f"sel work_schema, app_label from {project.test_schema}.vars_basic",
            fetch="one",
        )
        assert row[0] == "dbt_test_work"
        assert row[1] == "dbt_test_app"


# ---------------------------------------------------------------------------
# AC-4, AC-11: env_var()-based value injection into vars.yml
#
# dbt loads vars.yml as plain YAML (no Jinja rendering), so env_var() cannot
# be embedded in vars.yml values directly.  The supported pattern for CI
# injection is to read the environment variable at vars.yml generation time and
# write the resolved value into vars.yml.  These two tests exercise the
# default-value path (env var absent) and the CI-injection path (env var set).
# ---------------------------------------------------------------------------

_ENV_VAR_MODEL_SQL = """\
select '{{ var("injected_schema") }}' as injected_schema
"""

_ENV_VAR_NAME = "DBT_TEST_WORK_SCHEMA"


class TestVarsYmlEnvVarDefault:
    """AC-4, AC-11: env_var default is written into vars.yml when the env var is unset."""

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        mp = pytest.MonkeyPatch()
        mp.delenv(_ENV_VAR_NAME, raising=False)
        try:
            yield {"vars": {"injected_schema": os.getenv(_ENV_VAR_NAME, "default_work")}}
        finally:
            mp.undo()

    @pytest.fixture(scope="class")
    def models(self):
        return {"vars_env_var.sql": _ENV_VAR_MODEL_SQL}

    def test_env_var_default_used_when_unset(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        row = project.run_sql(
            f"sel injected_schema from {project.test_schema}.vars_env_var",
            fetch="one",
        )
        assert row[0] == "default_work"


class TestVarsYmlEnvVarCI:
    """AC-4, AC-11: CI-injected env var value is resolved into vars.yml at setup time."""

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        mp = pytest.MonkeyPatch()
        mp.setenv(_ENV_VAR_NAME, "ci_injected_schema")
        try:
            yield {"vars": {"injected_schema": os.getenv(_ENV_VAR_NAME, "default_work")}}
        finally:
            mp.undo()

    @pytest.fixture(scope="class")
    def models(self):
        return {"vars_env_var_ci.sql": _ENV_VAR_MODEL_SQL}

    def test_env_var_value_used_when_set(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        row = project.run_sql(
            f"sel injected_schema from {project.test_schema}.vars_env_var_ci",
            fetch="one",
        )
        assert row[0] == "ci_injected_schema"


# ---------------------------------------------------------------------------
# AC-4: vars.yml variables are available alongside other Jinja contexts
#
# dbt loads vars.yml as plain YAML; Jinja in vars.yml values is not
# evaluated.  However, vars.yml variables are available in the full model
# Jinja context alongside functions like target.name and env_var().
# ---------------------------------------------------------------------------

_TARGET_MODEL_SQL = """\
select '{{ var("env_prefix") }}_{{ target.name }}' as env_schema
"""


class TestVarsYmlTargetContext:
    """AC-4: vars.yml variable is available alongside {{ target }} in model Jinja."""

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        return {"vars": {"env_prefix": "dbt_temp"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"vars_target.sql": _TARGET_MODEL_SQL}

    def test_var_available_with_target_context(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        row = project.run_sql(
            f"sel env_schema from {project.test_schema}.vars_target",
            fetch="one",
        )
        # env_prefix from vars.yml is prepended; target.name is appended by Jinja
        assert row[0].startswith("dbt_temp_"), (
            f"Expected env_schema to start with 'dbt_temp_', got: {row[0]!r}"
        )
        # Jinja was evaluated — no raw brace sequences in the output
        assert "{{" not in row[0], (
            f"Jinja was not fully evaluated in model output: {row[0]!r}"
        )


# ---------------------------------------------------------------------------
# AC-5: Mutual exclusivity — vars in BOTH vars.yml and dbt_project.yml
#
# dbt 1.12 raises DbtProjectError when ANY variables are present in both
# vars.yml and dbt_project.yml's ``vars:`` section simultaneously.  We write
# vars.yml inside the test method (after the adapter fixture runs with an
# empty vars.yml) so that only the subsequent run_dbt call triggers the error.
# ---------------------------------------------------------------------------

_DUP_MODEL_SQL = "select 1 as id"


class TestVarsYmlDuplicateVarError:
    """AC-5: dbt rejects any variables defined in both vars.yml and dbt_project.yml."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "project_only_var": "from_project_yml",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"dup_var_model.sql": _DUP_MODEL_SQL}

    def test_duplicate_var_raises_error(self, project):
        # run_dbt always re-raises exceptions, so catch DbtProjectError directly.
        vars_yml_path = pathlib.Path(project.project_root) / "vars.yml"
        vars_yml_path.write_text(
            "vars:\n  vars_file_only: from_vars_yml\n", encoding="utf-8"
        )
        try:
            with pytest.raises(DbtProjectError, match="cannot be defined in both"):
                run_dbt(["run"])
        finally:
            vars_yml_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# AC-9: CLI --vars flag overrides vars.yml (environment-specific override)
# ---------------------------------------------------------------------------

_CLI_MODEL_SQL = """\
select '{{ var("override_target") }}' as override_target
"""


class TestVarsYmlCLIOverride:
    """AC-9: CLI --vars flag takes precedence over vars defined in vars.yml."""

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        return {"vars": {"override_target": "vars_yml_value"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"vars_cli_override.sql": _CLI_MODEL_SQL}

    def test_vars_yml_value_used_without_cli_override(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        row = project.run_sql(
            f"sel override_target from {project.test_schema}.vars_cli_override",
            fetch="one",
        )
        assert row[0] == "vars_yml_value"

    def test_cli_vars_override_vars_yml(self, project):
        results = run_dbt(
            ["run", "--vars", "{override_target: cli_override_value}"]
        )
        assert len(results) == 1
        assert results[0].status == "success"

        row = project.run_sql(
            f"sel override_target from {project.test_schema}.vars_cli_override",
            fetch="one",
        )
        assert row[0] == "cli_override_value"


# ---------------------------------------------------------------------------
# AC-6: Teradata-specific — temporary_metadata_generation_schema via vars.yml
# ---------------------------------------------------------------------------

_META_SEED_CSV = """\
id,name
1,alice
2,bob
""".lstrip()

_META_VIEW_SQL = """\
{{ config(materialized="view") }}
select * from {{ ref('vars_yml_meta_seed') }}
"""

_META_TABLE_SQL = """\
{{ config(materialized="incremental") }}
{% set view_relation = ref('vars_yml_meta_view') %}
{% set view_columns = adapter.get_columns_in_relation(view_relation) %}
select
    {{ view_columns|length }} as column_count,
    CURRENT_TIMESTAMP         as test_timestamp
"""


class TestVarsYmlTemporaryMetadataSchema:
    """AC-6: temporary_metadata_generation_schema sourced from vars.yml is honoured.

    Use the test's own schema (unique_schema) as the temporary metadata schema
    so dbt can create tables there without needing a separately provisioned DB.
    The fixture depends on unique_schema, which is resolved before vars.yml is
    written, so it is available during adapter fixture setup.
    """

    @pytest.fixture(scope="class")
    def vars_yml_update(self, unique_schema):
        # Put temporary_metadata_generation_schema directly in vars.yml.
        # Having a vars: section in project_config_update alongside vars.yml
        # would trigger the mutual-exclusivity error.
        return {"vars": {"temporary_metadata_generation_schema": unique_schema}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"vars_yml_meta_seed.csv": _META_SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "vars_yml_meta_view.sql": _META_VIEW_SQL,
            "vars_yml_meta_table.sql": _META_TABLE_SQL,
        }

    def test_temporary_metadata_schema_from_vars_yml(self, project):
        run_dbt(["seed"])

        log_dir = "log_output_vars_meta"
        (pathlib.Path(project.project_root) / log_dir).mkdir(parents=True, exist_ok=True)

        run_dbt(["--log-path", log_dir, "run"])

        log_text = (
            read_file(log_dir, "dbt.log")
            .replace("\n", " ")
            .replace("\\n", " ")
        )

        assert (
            f"temporary_metadata_generation_schema set to : {project.test_schema}"
            in log_text
        ), "vars.yml temporary_metadata_generation_schema was not loaded"

        schema = re.escape(project.test_schema)
        pattern = rf'"{schema}"\."vars_yml_meta_view_tmp_viw_tbl_\d+"'
        matches = re.findall(pattern, log_text)
        assert matches, (
            f"Expected temp table under schema {project.test_schema!r} "
            "(set via vars.yml temporary_metadata_generation_schema) to appear "
            "in run log, but none was found."
        )


# ---------------------------------------------------------------------------
# AC-2, AC-3, AC-7: Teradata-specific — query_band variables from vars.yml
#
# vars.yml variables are merged into the Jinja context used to render
# dbt_project.yml.  Values in the models: section (NOT the vars: section)
# can therefore reference var() calls that resolve from vars.yml.
# ---------------------------------------------------------------------------

_QB_VIEW_SQL = "sel 1 as id"

_QB_TABLE_SQL = """\
{{ config(materialized="table") }}
sel 2 as id
"""


class TestVarsYmlQueryBand:
    """AC-2, AC-3, AC-7: query_band model config uses vars from vars.yml."""

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        return {
            "vars": {
                "query_band_app": "dbt_vars_yml_test",
                "query_band_version": "2.0",
            }
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # The query_band value references vars from vars.yml via Jinja.
        # vars.yml is loaded before dbt_project.yml is rendered, so these
        # var() calls resolve at project-load time.  No ``vars:`` key here —
        # that would trigger the mutual-exclusivity error.
        return {
            "models": {
                "test": {
                    "+query_band": (
                        "app={{ var('query_band_app') }};"
                        "version={{ var('query_band_version') }};"
                    ),
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "qb_view_model.sql": _QB_VIEW_SQL,
            "qb_table_model.sql": _QB_TABLE_SQL,
        }

    def test_query_band_vars_from_vars_yml(self, project):
        results, logs = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 2
        assert all(r.status == "success" for r in results)
        assert "dbt_vars_yml_test" in logs, (
            "Expected query_band_app value 'dbt_vars_yml_test' in debug logs"
        )
        assert "version=2.0" in logs, (
            "Expected query_band_version value '2.0' in debug logs"
        )


# ---------------------------------------------------------------------------
# AC-8: Teradata-specific — table_kind from vars.yml
# ---------------------------------------------------------------------------

_TABLE_KIND_MODEL_SQL = """\
{{ config(materialized="table", table_kind=var("default_table_kind")) }}
sel 1 as id
"""


class TestVarsYmlTableKind:
    """AC-8: Teradata table_kind adapter config can be supplied via var() from vars.yml.

    Uses MULTISET (not SET) because CREATE TABLE AS SELECT (CTAS) produces a
    NoPI table, and Teradata forbids NoPI SET tables (Error 3706).  MULTISET
    is valid for NoPI CTAS tables and exercises the same var() → config path.
    """

    @pytest.fixture(scope="class")
    def vars_yml_update(self):
        return {"vars": {"default_table_kind": "MULTISET"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"vars_yml_table_kind.sql": _TABLE_KIND_MODEL_SQL}

    def test_table_kind_from_vars_yml(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert "CREATE MULTISET TABLE" in logs, (
            "Expected 'CREATE MULTISET TABLE' in debug logs when table_kind=MULTISET "
            "is sourced from vars.yml"
        )
