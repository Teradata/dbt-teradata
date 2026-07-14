"""
Functional tests for the dbt 1.11 `function` resource type on Teradata.

Tests require a live Teradata / Vantage instance (env vars in conftest.py).
Run with:
    pytest tests/functional/adapter/test_udf.py -v

Implementation notes:
  - dbt 1.11 ignores the `language:` key in functions.yml; FunctionNode.language
    always defaults to 'sql'. A language-guard test is not feasible.
  - `dbt run --select resource_type:function` does NOT pick up function nodes
    without downstream model dependents. Use `dbt build --select +<model>`
    so the function reaches the DAG as an upstream dependency.
  - target/compiled/ contains only the raw body (model.compiled_code).
    The full REPLACE FUNCTION DDL is in target/run/ after dbt build.
  - The dbt user must have CREATE FUNCTION privilege on the target schema; these
    tests grant it explicitly (dbt-teradata does not grant it automatically).
"""
import pathlib
import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_ADD_TWO_INTS_SQL = "RETURN a + b;"

_ADD_TWO_INTS_YML = """
functions:
  - name: add_two_ints
    config:
      type: scalar
      volatility: deterministic
    language: sql
    arguments:
      - name: a
        data_type: INTEGER
      - name: b
        data_type: INTEGER
    returns:
      data_type: INTEGER
""".lstrip()

_USE_UDF_SQL = "select {{ function('add_two_ints') }}(10, 32) as total"

_USE_UDF_YML = """
models:
  - name: use_udf
    columns:
      - name: total
        data_tests:
          - accepted_values:
              values: [42]
""".lstrip()

_CONCAT_STRINGS_SQL = "RETURN TRIM(prefix) || '_' || TRIM(suffix);"

_CONCAT_STRINGS_YML = """
functions:
  - name: concat_strings
    config:
      type: scalar
    language: sql
    arguments:
      - name: prefix
        data_type: VARCHAR(50)
      - name: suffix
        data_type: VARCHAR(50)
    returns:
      data_type: VARCHAR(101)
""".lstrip()


def _read_run_sql(project, project_name, function_name):
    """Read the fully-assembled REPLACE FUNCTION DDL from target/run/."""
    return (
        pathlib.Path(project.project_root)
        / "target" / "run" / project_name
        / "functions" / f"{function_name}.sql"
    ).read_text()


# ---------------------------------------------------------------------------
# Test: basic scalar UDF create + downstream model invocation
# ---------------------------------------------------------------------------

class TestScalarUdfBasic:
    """REPLACE FUNCTION is issued, function is callable from a model, test passes."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_scalar_udf_basic"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "add_two_ints.sql": _ADD_TWO_INTS_SQL,
            "functions.yml": _ADD_TWO_INTS_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "use_udf.sql": _USE_UDF_SQL,
            "schema.yml": _USE_UDF_YML,
        }

    def test_build_and_test(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")
        run_dbt(["build", "--select", "+use_udf"])


# ---------------------------------------------------------------------------
# Test: idempotency — second run must not error (REPLACE FUNCTION is safe)
# ---------------------------------------------------------------------------

class TestScalarUdfIdempotent:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_scalar_udf_idempotent"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "add_two_ints.sql": _ADD_TWO_INTS_SQL,
            "functions.yml": _ADD_TWO_INTS_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"use_udf.sql": _USE_UDF_SQL}

    def test_second_run_is_clean(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")
        run_dbt(["build", "--select", "+use_udf"])
        run_dbt(["build", "--select", "+use_udf"])


# ---------------------------------------------------------------------------
# Test: dbt list shows functions
# ---------------------------------------------------------------------------

class TestScalarUdfList:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_scalar_udf_list"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "add_two_ints.sql": _ADD_TWO_INTS_SQL,
            "functions.yml": _ADD_TWO_INTS_YML,
        }

    def test_list_resource_type(self, project):
        _, stdout = run_dbt_and_capture(["list", "--resource-type", "function"])
        assert "add_two_ints" in stdout


# ---------------------------------------------------------------------------
# Test: multiple argument types compile and execute correctly
# ---------------------------------------------------------------------------

_CONCAT_USE_SQL = "select {{ function('concat_strings') }}('hello', 'world') as concat_result"

_CONCAT_USE_YML = """
models:
  - name: use_concat
    columns:
      - name: concat_result
        data_tests:
          - accepted_values:
              values: ['hello_world']
""".lstrip()


class TestScalarUdfMultipleArgs:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_scalar_udf_multi_args"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "concat_strings.sql": _CONCAT_STRINGS_SQL,
            "functions.yml": _CONCAT_STRINGS_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "use_concat.sql": _CONCAT_USE_SQL,
            "schema.yml": _CONCAT_USE_YML,
        }

    def test_varchar_args(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")
        run_dbt(["build", "--select", "+use_concat"])


# ---------------------------------------------------------------------------
# Test: CONTAINS SQL is always emitted (the only valid data access for SQL UDFs)
# ---------------------------------------------------------------------------
# Teradata SQL UDFs with LANGUAGE SQL / INLINE TYPE 1 only support CONTAINS SQL.
# NO SQL, READS SQL DATA, MODIFIES SQL DATA are for external (C/Java) UDFs.

_CONTAINS_SQL_YML = """
functions:
  - name: contains_sql_udf
    config:
      type: scalar
    language: sql
    arguments:
      - name: x
        data_type: INTEGER
    returns:
      data_type: INTEGER
""".lstrip()

_CONTAINS_SQL_BODY = "RETURN x + 1;"
_CONTAINS_SQL_USE = "select {{ function('contains_sql_udf') }}(1) as udf_val"


class TestSqlDataAccessConfig:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_udf_data_access"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "contains_sql_udf.sql": _CONTAINS_SQL_BODY,
            "functions.yml": _CONTAINS_SQL_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"use_contains_sql.sql": _CONTAINS_SQL_USE}

    def test_contains_sql_in_run_sql(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")
        run_dbt(["build", "--select", "+use_contains_sql"])
        ddl = _read_run_sql(project, "test_udf_data_access", "contains_sql_udf")
        assert "CONTAINS SQL" in ddl


# ---------------------------------------------------------------------------
# Test: volatility: deterministic renders DETERMINISTIC in executed DDL
# ---------------------------------------------------------------------------

_VOLATILE_YML = """
functions:
  - name: det_udf
    config:
      type: scalar
      volatility: deterministic
    language: sql
    arguments:
      - name: x
        data_type: INTEGER
    returns:
      data_type: INTEGER
""".lstrip()

_VOLATILE_SQL = "RETURN x * 2;"
_VOLATILE_USE = "select {{ function('det_udf') }}(5) as val"


class TestVolatilityConfig:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_udf_volatility"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "det_udf.sql": _VOLATILE_SQL,
            "functions.yml": _VOLATILE_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"use_det_udf.sql": _VOLATILE_USE}

    def test_deterministic_in_run_sql(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")
        run_dbt(["build", "--select", "+use_det_udf"])
        ddl = _read_run_sql(project, "test_udf_volatility", "det_udf")
        assert "DETERMINISTIC" in ddl


# ---------------------------------------------------------------------------
# Test: default sql_data_access is CONTAINS SQL (not omitted, not NO SQL)
# ---------------------------------------------------------------------------

_DEFAULT_ACCESS_YML = """
functions:
  - name: default_access_udf
    config:
      type: scalar
    language: sql
    arguments:
      - name: x
        data_type: INTEGER
    returns:
      data_type: INTEGER
""".lstrip()

_DEFAULT_ACCESS_SQL = "RETURN x + 0;"
_DEFAULT_ACCESS_USE = "select {{ function('default_access_udf') }}(1) as val"


class TestDefaultSqlDataAccess:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_udf_default_access"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "default_access_udf.sql": _DEFAULT_ACCESS_SQL,
            "functions.yml": _DEFAULT_ACCESS_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"use_default_access.sql": _DEFAULT_ACCESS_USE}

    def test_default_access_is_contains_sql(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")
        run_dbt(["build", "--select", "+use_default_access"])
        ddl = _read_run_sql(project, "test_udf_default_access", "default_access_udf")
        assert "CONTAINS SQL" in ddl


# ---------------------------------------------------------------------------
# Test: `grants` on a UDF is applied as GRANT EXECUTE FUNCTION
# ---------------------------------------------------------------------------
# The grant DCL is executed at runtime (not written to target/run/), so we
# verify it by confirming the EXECUTE FUNCTION right ('EF') actually lands in
# DBC.AllRightsV. The old behaviour emitted plain `GRANT EXECUTE`, which is the
# macro/stored-procedure privilege and would not create an 'EF' right on a UDF.

_GRANTED_UDF_YML = """
functions:
  - name: granted_udf
    config:
      type: scalar
      grants:
        execute: ['PUBLIC']
    language: sql
    arguments:
      - name: x
        data_type: INTEGER
    returns:
      data_type: INTEGER
""".lstrip()

_GRANTED_UDF_SQL = "RETURN x + 1;"
_GRANTED_UDF_USE = "select {{ function('granted_udf') }}(1) as val"


class TestScalarUdfGrants:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_udf_grants"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "granted_udf.sql": _GRANTED_UDF_SQL,
            "functions.yml": _GRANTED_UDF_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"use_granted_udf.sql": _GRANTED_UDF_USE}

    def test_execute_function_grant_applied(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")
        run_dbt(["build", "--select", "+use_granted_udf"])
        rights = project.run_sql(
            "SELECT AccessRight FROM DBC.AllRightsV "
            f"WHERE DatabaseName='{project.test_schema}' "
            "AND TRIM(LOWER(TableName))='granted_udf' "
            "AND TRIM(LOWER(Username))='public' "
            "AND AccessRight='EF'",
            fetch="all",
        )
        assert len(rights) >= 1


# ---------------------------------------------------------------------------
# Test: aggregate UDFs are rejected with a clear compile-time error
# ---------------------------------------------------------------------------
# `--select +use_aggregate_udf` pulls in two nodes: the function (which errors)
# and the downstream model (which gets marked 'skipped' as a result), so
# `results` has 2 entries here — assert on content across all of them rather
# than an exact list length.

_AGGREGATE_YML = """
functions:
  - name: aggregate_udf
    config:
      type: aggregate
    language: sql
    arguments:
      - name: x
        data_type: INTEGER
    returns:
      data_type: INTEGER
""".lstrip()

_AGGREGATE_SQL = "RETURN x;"
_AGGREGATE_USE = "select {{ function('aggregate_udf') }}(1) as val"


class TestAggregateUdfNotSupported:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_udf_aggregate_not_supported"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "aggregate_udf.sql": _AGGREGATE_SQL,
            "functions.yml": _AGGREGATE_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"use_aggregate_udf.sql": _AGGREGATE_USE}

    def test_aggregate_udf_raises_clear_error(self, project):
        # expect_pass=False (rather than pytest.raises(CompilationError)) is safe here:
        # BaseRunner.safe_run in dbt-core catches any Exception — including the
        # CompilationError raised by raise_compiler_error — around compile_and_execute
        # and converts it into a per-node error result instead of letting it propagate.
        results = run_dbt(["build", "--select", "+use_aggregate_udf"], expect_pass=False)
        assert any("Aggregate user-defined functions" in str(r.message or "") for r in results)


# ---------------------------------------------------------------------------
# Test: persist_docs relation-level description writes COMMENT ON FUNCTION
# ---------------------------------------------------------------------------
# Teradata stores UDF comments in DBC.TablesV.CommentString, the same column
# used for tables/views/macros (TableKind='F' for a scalar UDF), so the comment
# is verified directly against DBC.TablesV. Function nodes are not "relational"
# in dbt-core 1.11 (is_relational excludes NodeType.Function), so they are never
# fetched by `dbt docs generate` and therefore do not surface in catalog.json.

_DOC_UDF_SQL = "RETURN x + 1;"
_DOC_UDF_USE = "select {{ function('doc_udf') }}(1) as val"

_DOC_UDF_DESCRIPTION = "Adds one to the input integer."
_DOC_UDF_DESCRIPTION_V2 = "Adds one to the given integer value."


def _doc_udf_yml(description):
    return (
        "functions:\n"
        "  - name: doc_udf\n"
        "    config:\n"
        "      type: scalar\n"
        "      persist_docs:\n"
        "        relation: true\n"
        f"    description: \"{description}\"\n"
        "    language: sql\n"
        "    arguments:\n"
        "      - name: x\n"
        "        data_type: INTEGER\n"
        "    returns:\n"
        "      data_type: INTEGER\n"
    )


def _function_comment(project, function_name):
    """Read the current COMMENT ON FUNCTION text straight from DBC.TablesV."""
    row = project.run_sql(
        "SELECT CommentString FROM DBC.TablesV "
        f"WHERE DatabaseName='{project.test_schema}' "
        "AND TableKind='F' "
        f"AND TRIM(LOWER(TableName))='{function_name}'",
        fetch="one",
    )
    return row[0].strip() if row and row[0] is not None else None


class TestScalarUdfPersistDocsRelation:
    """persist_docs: {relation: true} + a `description:` writes COMMENT ON FUNCTION."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_udf_persist_docs"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "doc_udf.sql": _DOC_UDF_SQL,
            "functions.yml": _doc_udf_yml(_DOC_UDF_DESCRIPTION),
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"use_doc_udf.sql": _DOC_UDF_USE}

    def test_comment_on_function_written(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")
        _, logs = run_dbt_and_capture(["--debug", "build", "--select", "+use_doc_udf"])
        assert "comment on function" in logs.lower()
        assert _function_comment(project, "doc_udf") == _DOC_UDF_DESCRIPTION


# ---------------------------------------------------------------------------
# Test: unchanged description issues no COMMENT ON FUNCTION DDL on rerun
# ---------------------------------------------------------------------------
# Unlike REPLACE VIEW / CREATE OR REPLACE TABLE (which drop-and-recreate the
# object, wiping DBC.TablesV.CommentString), Teradata's REPLACE FUNCTION
# preserves the existing comment across a rebuild with an unchanged signature
# and body -- verified empirically: COMMENT ON FUNCTION, then REPLACE FUNCTION
# again with identical DDL, leaves CommentString unchanged. So change detection
# in teradata__persist_docs (existing_rel_comment != model.description) is
# meaningful here, unlike for table/view (see TestPersistDocsIdempotentTeradata
# in teradata_dbt/test_validate_teradata_persist_docs.py, which needs an
# *incremental* model for the same reason table/view do not preserve comments).

class TestScalarUdfPersistDocsIdempotent:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_udf_persist_docs_idempotent"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "doc_udf.sql": _DOC_UDF_SQL,
            "functions.yml": _doc_udf_yml(_DOC_UDF_DESCRIPTION),
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"use_doc_udf.sql": _DOC_UDF_USE}

    def test_no_ddl_on_unchanged_rerun(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")
        run_dbt(["build", "--select", "+use_doc_udf"])
        assert _function_comment(project, "doc_udf") == _DOC_UDF_DESCRIPTION

        # Second run: same functions.yml, same description -> no COMMENT DDL.
        # COMMENT DDL is only emitted at DEBUG level, so capture with --debug.
        _, logs = run_dbt_and_capture(["--debug", "build", "--select", "+use_doc_udf"])
        assert "comment on function" not in logs.lower()
        assert _function_comment(project, "doc_udf") == _DOC_UDF_DESCRIPTION


# ---------------------------------------------------------------------------
# Test: changing the description re-issues COMMENT ON FUNCTION with new text
# ---------------------------------------------------------------------------

class TestScalarUdfPersistDocsChanged:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_udf_persist_docs_changed"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "doc_udf.sql": _DOC_UDF_SQL,
            "functions.yml": _doc_udf_yml(_DOC_UDF_DESCRIPTION),
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"use_doc_udf.sql": _DOC_UDF_USE}

    def test_changed_description_reissues_ddl(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")
        run_dbt(["build", "--select", "+use_doc_udf"])
        assert _function_comment(project, "doc_udf") == _DOC_UDF_DESCRIPTION

        functions_yml_path = (
            pathlib.Path(project.project_root) / "functions" / "functions.yml"
        )
        functions_yml_path.write_text(_doc_udf_yml(_DOC_UDF_DESCRIPTION_V2))

        _, logs = run_dbt_and_capture(["--debug", "build", "--select", "+use_doc_udf"])
        assert "comment on function" in logs.lower()
        assert _function_comment(project, "doc_udf") == _DOC_UDF_DESCRIPTION_V2


# ---------------------------------------------------------------------------
# Note: persist_docs `columns` config on a function (warn-and-skip) is NOT
# covered functionally here. Verified empirically that dbt-core 1.11's
# UnparsedFunctionUpdate schema (HasColumnProps, not HasColumnDocs) does not
# parse a `columns:` block into model.columns for function nodes -- a
# functions.yml `columns:` list is silently dropped, so the warn-and-skip
# branch in teradata__persist_docs can never be reached via YAML in practice.
# That branch is instead covered directly in tests/unit/test_persist_docs.py
# (TestPersistDocsFunctionColumnsSkipped), which invokes the macro with a
# synthetic model.columns dict.
