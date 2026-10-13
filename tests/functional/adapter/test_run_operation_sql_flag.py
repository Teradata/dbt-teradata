"""
Functional tests for the --sql flag on `dbt run-operation` (dbt 1.12+).

The --sql flag lets you execute ad-hoc SQL with full Jinja compilation
(ref(), source(), var(), target) without writing a macro file.

Tests require a live Teradata / Vantage instance (env vars in conftest.py).
Run with:
    pytest tests/functional/adapter/test_run_operation_sql_flag.py -v

Implementation notes:
  - Model is materialized as 'table' (not view) because COLLECT STATISTICS
    and DELETE...ALL are not supported on Teradata views.
  - Model is named 'test_data' (not 'sample') because SAMPLE is a Teradata
    reserved word; schema.sample causes parser error 3707 in CTAS / DDL.
  - Staging tables for DELETE tests are cloned from the 'base' seed table
    (a guaranteed physical table) rather than from the model.
"""
import pytest
import dbt.version as _dbt_version
from packaging.version import Version
from dbt.tests.util import run_dbt, run_dbt_and_capture

_dbt_version_tuple = Version(_dbt_version.__version__).release[:2]

pytestmark = pytest.mark.skipif(
    _dbt_version_tuple < (1, 12),
    reason=f"Requires dbt-core >= 1.12 (found {_dbt_version.__version__})",
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_SEEDS_BASE_CSV = """id,name,amount
1,Alice,100
2,Bob,200
3,Carol,300
""".lstrip()

# 'table' materialization required: COLLECT STATISTICS and DELETE...ALL
# are not supported on Teradata views.
_MODEL_TEST_DATA_SQL = """
{{ config(materialized='table') }}
select id, name, amount from {{ ref('base') }}
""".lstrip()

_SCHEMA_SOURCES_YML = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: base
""".lstrip()


# ---------------------------------------------------------------------------
# Test: basic --sql execution
# ---------------------------------------------------------------------------

class TestRunOperationSqlFlagBasic:
    """Verify that --sql executes ad-hoc SQL on Teradata without a macro."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_sql_flag_basic"}

    def test_basic_select(self, project):
        """--sql flag executes a simple SELECT on Teradata."""
        run_dbt(["run-operation", "--sql", "SELECT 1 AS one"])

    def test_basic_select_with_teradata_dialect(self, project):
        """--sql flag executes Teradata-dialect SQL (SEL shorthand)."""
        run_dbt(["run-operation", "--sql", "SEL CURRENT_DATE AS today"])


# ---------------------------------------------------------------------------
# Test: Jinja context in --sql (ref, source, var, target)
# ---------------------------------------------------------------------------

class TestRunOperationSqlFlagJinjaContext:
    """Verify that Jinja compilation context is available inside --sql."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_sql_flag_jinja"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": _SEEDS_BASE_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_data.sql": _MODEL_TEST_DATA_SQL,
            "schema.yml": _SCHEMA_SOURCES_YML,
        }

    def test_ref_resolution(self, project):
        """ref() resolves to the correct schema-qualified relation in --sql."""
        run_dbt(["seed"])
        run_dbt(["run"])
        _, out = run_dbt_and_capture(
            ["--debug", "run-operation", "--sql",
             "SELECT COUNT(*) AS cnt FROM {{ ref('test_data') }}"]
        )
        # Compiled SQL must contain the test schema — proves ref() resolved correctly.
        assert project.test_schema in out

    def test_source_resolution(self, project):
        """source() resolves to the correct schema-qualified relation in --sql."""
        run_dbt(["seed"])
        _, out = run_dbt_and_capture(
            ["--debug", "run-operation", "--sql",
             "SELECT COUNT(*) AS cnt FROM {{ source('raw', 'base') }}"]
        )
        # Compiled SQL must contain the test schema — proves source() resolved correctly.
        assert project.test_schema in out

    def test_var_resolution(self, project):
        """var() injects the supplied value into the compiled SQL."""
        _, out = run_dbt_and_capture(
            [
                "--debug",
                "run-operation",
                "--sql", "SELECT '{{ var(\"my_label\") }}' AS label",
                "--vars", "{my_label: hello_teradata}",
            ]
        )
        assert "hello_teradata" in out

    def test_target_context(self, project):
        """target.schema and target.type compile to the correct values in --sql."""
        _, out = run_dbt_and_capture(
            ["--debug", "run-operation", "--sql",
             "SELECT '{{ target.schema }}' AS schema_name, '{{ target.type }}' AS adapter_type"]
        )
        assert project.test_schema in out
        assert "teradata" in out.lower()

    def test_conditional_jinja(self, project):
        """Jinja if/else takes the correct branch when target.type == 'teradata'."""
        _, out = run_dbt_and_capture(
            [
                "--debug",
                "run-operation",
                "--sql",
                (
                    "{% if target.type == 'teradata' %}"
                    "SELECT 1 AS is_teradata"
                    "{% else %}"
                    "SELECT 0 AS is_teradata"
                    "{% endif %}"
                ),
            ]
        )
        # The teradata branch must appear in the compiled SQL; the else branch must not.
        assert "1 AS is_teradata" in out
        assert "0 AS is_teradata" not in out

    def test_jinja_loop(self, project):
        """Jinja for-loop emits the correct arithmetic expression in the compiled SQL."""
        _, out = run_dbt_and_capture(
            [
                "--debug",
                "run-operation",
                "--sql",
                (
                    "SELECT "
                    "{% for i in [1, 2, 3] %}"
                    "{{ i }}{% if not loop.last %} + {% endif %}"
                    "{% endfor %}"
                    " AS total"
                ),
            ]
        )
        # Loop must have emitted "1 + 2 + 3" in the compiled SQL.
        assert "1 + 2 + 3" in out


# ---------------------------------------------------------------------------
# Test: Teradata-specific SQL via --sql flag
# ---------------------------------------------------------------------------

class TestRunOperationSqlFlagTeradataSpecific:
    """Verify Teradata-specific administrative SQL executes via --sql."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_sql_flag_teradata"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": _SEEDS_BASE_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_data.sql": _MODEL_TEST_DATA_SQL,
            "schema.yml": _SCHEMA_SOURCES_YML,
        }

    def test_collect_statistics(self, project):
        """COLLECT STATISTICS executes on a ref()-resolved physical table via --sql."""
        run_dbt(["seed"])
        run_dbt(["run"])
        # COLUMN clause is required when no statistics are pre-defined on the table;
        # bare 'COLLECT STATISTICS ON table' only refreshes existing definitions
        # and raises error 3624 if none exist.
        run_dbt(
            ["run-operation", "--sql",
             "COLLECT STATISTICS COLUMN (id) ON {{ ref('test_data') }}"]
        )
        # Verify the stat was actually written to the data dictionary.
        stats = project.run_sql(
            f"SELECT COUNT(*) AS cnt FROM DBC.StatsV "
            f"WHERE TRIM(LOWER(DatabaseName))=LOWER('{project.test_schema}') "
            f"AND TRIM(LOWER(TableName))='test_data' "
            f"AND TRIM(LOWER(ColumnName))='id'",
            fetch="one",
        )
        assert int(stats[0]) >= 1

    def test_query_band_set_for_session(self, project):
        """SET QUERY_BAND is acknowledged by Teradata via --sql flag."""
        _, out = run_dbt_and_capture(
            [
                "run-operation",
                "--sql",
                "SET QUERY_BAND = 'app=dbt_sql_flag;workload=test;' FOR SESSION",
            ]
        )
        # Teradata confirms SET QUERY_BAND with activity "Set Query_Band" in the response.
        assert "Set Query_Band" in out

    def test_query_band_with_var(self, project):
        """SET QUERY_BAND with var() is acknowledged by Teradata via --sql flag."""
        _, out = run_dbt_and_capture(
            [
                "--debug",
                "run-operation",
                "--sql",
                "SET QUERY_BAND = 'app=dbt;workload={{ var(\"workload\") }};' FOR SESSION",
                "--vars", "{workload: nightly_etl}",
            ]
        )
        assert "Set Query_Band" in out
        assert "workload=nightly_etl" in out

    def test_delete_all_with_literal_path(self, project):
        """DELETE...ALL with a literal table path via --sql flag."""
        run_dbt(["seed"])
        # Clone base (the seed — a guaranteed physical table) into a staging table.
        project.run_sql(
            f"CREATE MULTISET TABLE {project.test_schema}.staging_delete_test AS "
            f"(SELECT * FROM {project.test_schema}.base) WITH DATA"
        )
        try:
            run_dbt(
                [
                    "run-operation",
                    "--sql",
                    f"DELETE {project.test_schema}.staging_delete_test ALL",
                ]
            )
            row_count = project.run_sql(
                f"SELECT COUNT(*) AS cnt FROM {project.test_schema}.staging_delete_test",
                fetch="one",
            )
            assert int(row_count[0]) == 0
        finally:
            project.run_sql(f"DROP TABLE {project.test_schema}.staging_delete_test")



# ---------------------------------------------------------------------------
# Test: DELETE...ALL with ref() — isolated class to prevent cross-test state
# ---------------------------------------------------------------------------
# This test is kept in its own class so the DELETE ALL on test_data cannot
# affect any other test method.  dbt's test harness creates a unique schema
# per class (unique_schema fixture has scope="class"), so the truncation is
# fully contained here regardless of pytest execution order.

class TestRunOperationSqlFlagDeleteWithRef:
    """Verify DELETE...ALL with ref() in an isolated class."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_sql_flag_delete_ref"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": _SEEDS_BASE_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_data.sql": _MODEL_TEST_DATA_SQL,
            "schema.yml": _SCHEMA_SOURCES_YML,
        }

    def test_delete_all_with_ref(self, project):
        """DELETE...ALL targets a ref()-resolved physical table via --sql flag."""
        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(
            ["run-operation", "--sql", "DELETE {{ ref('test_data') }} ALL"]
        )
        row_count = project.run_sql(
            f"SELECT COUNT(*) AS cnt FROM {project.test_schema}.test_data",
            fetch="one",
        )
        assert int(row_count[0]) == 0


# ---------------------------------------------------------------------------
# Test: error handling
# ---------------------------------------------------------------------------

class TestRunOperationSqlFlagErrorHandling:
    """Verify proper error handling for invalid SQL and bad ref() lookups."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_sql_flag_errors"}

    def test_invalid_sql_fails(self, project):
        """Invalid SQL causes run-operation to fail with expect_pass=False."""
        run_dbt(
            ["run-operation", "--sql", "THIS IS NOT VALID SQL AT ALL"],
            expect_pass=False,
        )

    def test_nonexistent_ref_fails(self, project):
        """ref() targeting a model that does not exist causes a compile-time error."""
        run_dbt(
            ["run-operation", "--sql", "SELECT * FROM {{ ref('this_model_does_not_exist') }}"],
            expect_pass=False,
        )

    def test_invalid_sql_error_message_captured(self, project):
        """Error output from an invalid --sql statement contains an error indicator."""
        results, out = run_dbt_and_capture(
            ["run-operation", "--sql", "GARBAGE SQL THAT FAILS"],
            expect_pass=False,
        )
        assert any(r.status in ("error", "fail") for r in results)
        assert out and ("error" in out.lower() or "syntax" in out.lower())


# ---------------------------------------------------------------------------
# Test: multiple --sql invocations in sequence
# ---------------------------------------------------------------------------

class TestRunOperationSqlFlagSequence:
    """Verify multiple sequential --sql calls all succeed."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_sql_flag_sequence"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": _SEEDS_BASE_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_data.sql": _MODEL_TEST_DATA_SQL,
            "schema.yml": _SCHEMA_SOURCES_YML,
        }

    def test_multiple_sql_in_sequence(self, project):
        """Multiple --sql invocations in sequence all execute successfully."""
        run_dbt(["seed"])
        run_dbt(["run"])

        run_dbt(["run-operation", "--sql", "SELECT 1 AS step_one"])
        run_dbt(
            ["run-operation", "--sql", "SELECT COUNT(*) AS cnt FROM {{ ref('test_data') }}"]
        )
        run_dbt(
            [
                "run-operation",
                "--sql",
                "SET QUERY_BAND = 'app=dbt_sequence;step=three;' FOR SESSION",
            ]
        )
        run_dbt(["run-operation", "--sql",
                 "COLLECT STATISTICS COLUMN (id) ON {{ ref('test_data') }}"])
