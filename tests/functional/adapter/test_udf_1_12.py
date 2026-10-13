"""Functional tests for IDE-26478: dbt 1.12 UDF `overloads` and Python UDF certification.

dbt Core 1.12 adds two UDF-related features that need certification against
Teradata:

  overloads
    A `functions.yml` entry may declare `overloads:`, each pointing (via
    `defined_in:`) at a sibling `.sql` file with a different argument/return
    signature. dbt-core's parser "absorbs" each overload's SQL body into the
    root FunctionNode (removing the overload file as a standalone DAG node),
    then `FunctionRunner.execute()` invokes the *same* materialization macro
    once per signature (root + each overload), holding `target_relation`
    constant. Since `teradata__scalar_function_sql` only ever reads
    `model.arguments` / `model.returns.data_type` / `model.compiled_code` (and
    Teradata natively supports SQL function overloading -- same name, distinct
    signatures), this works correctly with **no adapter code changes**. The
    test below proves both signatures are created and independently callable.

  Python UDFs (in-database Python UDFs / PyPI packages)
    dbt-teradata's `{% materialization function, adapter='teradata',
    supported_languages=['sql'] %}` declaration is read by dbt-core's
    `FunctionRunner._check_lang_supported()` *before* macro dispatch is even
    attempted. Requesting `language: python` therefore already fails cleanly
    with a clear, adapter-specific message identifying the unsupported
    language -- no new guard macro is required. The test below asserts that
    error message (via substring match) as a certification/regression guard.

Tests require a live Teradata / Vantage instance (env vars in conftest.py).
Run with:
    pytest tests/functional/adapter/test_udf_1_12.py -v

All tests in this module are skipped on dbt-core < 1.12 (module-level
pytestmark) since `overloads` is only available starting in dbt-core 1.12.
"""
import pytest
import dbt.version as _dbt_version
from packaging.version import Version

from dbt.tests.util import run_dbt

_dbt_version_tuple = Version(_dbt_version.__version__).release[:2]

pytestmark = pytest.mark.skipif(
    _dbt_version_tuple < (1, 12),
    reason=f"Requires dbt-core >= 1.12 (found {_dbt_version.__version__})",
)

# ---------------------------------------------------------------------------
# Test: `overloads` - multiple signatures for one function name
# ---------------------------------------------------------------------------

_MULTIPLY_INT_SQL = "RETURN x * y;"
_MULTIPLY_DECIMAL_SQL = _MULTIPLY_INT_SQL

_MULTIPLY_OVERLOADS_YML = """
functions:
  - name: multiply_values
    config:
      type: scalar
      volatility: deterministic
    language: sql
    arguments:
      - name: x
        data_type: INTEGER
      - name: y
        data_type: INTEGER
    returns:
      data_type: INTEGER
    overloads:
      - defined_in: multiply_values_decimal
        arguments:
          - name: x
            data_type: "DECIMAL(10,2)"
          - name: y
            data_type: "DECIMAL(10,2)"
        returns:
          data_type: "DECIMAL(10,2)"
""".lstrip()

_USE_OVERLOADS_SQL = """
select
    {{ function('multiply_values') }}(3, 4) as int_result,
    {{ function('multiply_values') }}(3.50, 2.00) as decimal_result
""".lstrip()

_USE_OVERLOADS_YML = """
models:
  - name: use_overloaded_udf
    columns:
      - name: int_result
        data_tests:
          - accepted_values:
              values: [12]
""".lstrip()


class TestScalarUdfOverloads:
    """A single `overloads` entry creates both signatures and both are callable."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_scalar_udf_overloads"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "multiply_values.sql": _MULTIPLY_INT_SQL,
            "multiply_values_decimal.sql": _MULTIPLY_DECIMAL_SQL,
            "functions.yml": _MULTIPLY_OVERLOADS_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "use_overloaded_udf.sql": _USE_OVERLOADS_SQL,
            "schema.yml": _USE_OVERLOADS_YML,
        }

    def test_overloads_build_and_execute(self, project):
        username = project.adapter.config.credentials.username
        project.run_sql(f"GRANT CREATE FUNCTION ON {project.test_schema} TO {username}")

        # Only one DAG node is produced -- the overload file is absorbed into
        # the root function's node, not run as an independent resource.
        results = run_dbt(["build", "--select", "+use_overloaded_udf"])
        function_results = [r for r in results if r.node.resource_type == "function"]
        assert len(function_results) == 1
        assert function_results[0].node.name == "multiply_values"

        # Both signatures are independently queryable, proving Teradata's
        # native SQL function overloading round-trips through dbt correctly.
        row = project.run_sql(
            f"SELECT {project.test_schema}.multiply_values(3, 4)", fetch="one"
        )
        assert int(row[0]) == 12

        row = project.run_sql(
            f"SELECT {project.test_schema}.multiply_values(3.50, 2.00)", fetch="one"
        )
        assert float(row[0]) == 7.0


# ---------------------------------------------------------------------------
# Test: Python UDFs are rejected with a clear, adapter-specific error
# ---------------------------------------------------------------------------

_PY_UDF_SQL = """
def price_for_xlarge(price):
    return price * 2
""".lstrip()

_PY_UDF_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item.
    language: python
    config:
      entry_point: price_for_xlarge
      runtime_version: "3.11"
    arguments:
      - name: price
        data_type: FLOAT
    returns:
      data_type: FLOAT
""".lstrip()

_USE_PY_UDF_SQL = "select {{ function('price_for_xlarge') }}(100) as val"


class TestPythonUdfNotSupported:
    """Python UDFs are declared unsupported via `supported_languages=['sql']`,
    so dbt-core rejects them before macro dispatch with a clear message."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "test_udf_python_not_supported"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.py": _PY_UDF_SQL,
            "functions.yml": _PY_UDF_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"use_price_for_xlarge.sql": _USE_PY_UDF_SQL}

    def test_python_udf_raises_clear_error(self, project):
        # expect_pass=False is used here (rather than pytest.raises) because
        # BaseRunner.safe_run in dbt-core converts the raised exception into a
        # per-node error result instead of letting it propagate.
        results = run_dbt(
            ["build", "--select", "+use_price_for_xlarge"], expect_pass=False
        )
        function_results = [r for r in results if r.node.resource_type == "function"]
        assert len(function_results) == 1
        assert any(
            "only supports languages" in str(r.message or "")
            and "python" in str(r.message or "")
            for r in function_results
        )
