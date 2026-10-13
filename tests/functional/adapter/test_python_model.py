"""Functional tests for client-side Python model support (IDE-24196).

These require a live Teradata instance (same as the rest of tests/functional).
They exercise the real end-to-end path: dbt compiles the .py model, the adapter
executes it client-side, reads upstream relations into pandas over teradatasql,
and writes the resulting DataFrame back.

Models are written with the **pandas** API (not Spark/Snowpark), which is what
the client-side runtime provides. Upstream data comes from a seed (Teradata
rejects FROM-less ``SELECT ... UNION ALL`` literals), matching real usage.
"""

import pytest

from dbt.tests.util import run_dbt, check_relations_equal


# --------------------------------------------------------------------------- #
# Fixtures / model bodies
# --------------------------------------------------------------------------- #
orders_seed_csv = """order_id,customer_id,amount,status
1,101,10.5,completed
2,101,20.0,completed
3,102,,pending
4,103,30.25,completed
5,102,5.75,cancelled
6,103,15.0,completed
"""

customers_seed_csv = """customer_id,customer_name,region
101,Alice,East
102,Bob,West
103,Carol,East
"""

customers_upstream_sql = """
{{ config(materialized='table') }}
select customer_id, customer_name, region
from {{ ref('customers_seed') }}
"""

# A SINGLE python model that references MULTIPLE tables (two refs) and joins
# them in pandas. This is the case the SCRIPT Table Operator could not support.
py_multi_ref_model = """
def model(dbt, session):
    dbt.config(materialized='table')
    orders = dbt.ref('upstream')            # table 1
    customers = dbt.ref('customers')        # table 2
    joined = orders.merge(customers, on='customer_id', how='inner')
    return joined[['order_id', 'customer_id', 'customer_name', 'region', 'status']]
"""

upstream_sql = """
{{ config(materialized='table') }}
select order_id, customer_id, amount, status
from {{ ref('orders_seed') }}
"""

# Python model that refs a SQL model and filters with pandas.
py_ref_model = """
def model(dbt, session):
    dbt.config(materialized='table')
    df = dbt.ref('upstream')
    return df[df['status'] == 'completed'][['order_id', 'customer_id', 'amount']]
"""

# Teradata-valid expected relation (selected from a table).
expected_completed_sql = """
{{ config(materialized='table') }}
select order_id, customer_id, amount
from {{ ref('orders_seed') }}
where status = 'completed'
"""

# Python model built from scratch covering multiple dtypes incl. nulls.
py_scratch_model = """
import pandas as pd
import numpy as np

def model(dbt, session):
    dbt.config(materialized='table')
    return pd.DataFrame({
        'i': [1, 2, 3],
        'f': [1.5, 2.5, np.nan],
        's': ['x', None, 'zzz'],
        'b': [True, False, True],
    })
"""

# Python model that refs another python model (python -> python chain).
py_chain_model = """
def model(dbt, session):
    dbt.config(materialized='table')
    df = dbt.ref('py_ref').copy()
    df['amount_x2'] = df['amount'] * 2
    return df
"""

# SQL model that refs a python model (python -> sql chain, mixed DAG).
sql_on_py_model = """
{{ config(materialized='table') }}
select * from {{ ref('py_ref') }}
"""

# Python model reading from a dbt source().
source_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: orders_seed
"""

py_source_model = """
def model(dbt, session):
    dbt.config(materialized='table')
    df = dbt.source('raw', 'orders_seed')
    return df[['order_id', 'amount']]
"""

# Incremental python model — NOT supported in the MVP (table only).
py_incremental_model = """
def model(dbt, session):
    dbt.config(materialized='incremental')
    return dbt.ref('upstream')
"""


class TestPythonModelClientSide:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "orders_seed.csv": orders_seed_csv,
            "customers_seed.csv": customers_seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sources.yml": source_yml,
            "upstream.sql": upstream_sql,
            "customers.sql": customers_upstream_sql,
            "py_ref.py": py_ref_model,
            "expected_completed.sql": expected_completed_sql,
            "py_scratch.py": py_scratch_model,
            "py_chain.py": py_chain_model,
            "sql_on_py.sql": sql_on_py_model,
            "py_source.py": py_source_model,
            "py_multi_ref.py": py_multi_ref_model,
        }

    def test_build_all(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        # upstream, customers, py_ref, expected_completed, py_scratch, py_chain,
        # sql_on_py, py_source, py_multi_ref
        assert len(results) == 9

    def test_multi_ref_join(self, project):
        """A single python model that refs TWO tables and joins them."""
        run_dbt(["seed"])
        run_dbt(["run"])
        rel = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="py_multi_ref"
        )
        # 6 orders all have a matching customer -> 6 joined rows
        assert project.run_sql(f"select count(*) from {rel}", fetch="one")[0] == 6
        # every row got a non-null customer_name from the second table
        assert (
            project.run_sql(
                f"select count(*) from {rel} where customer_name is null", fetch="one"
            )[0]
            == 0
        )

    def test_ref_filter_matches_expected(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        check_relations_equal(project.adapter, ["py_ref", "expected_completed"])

    def test_scratch_model_row_count_and_nulls(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "-s", "py_scratch"])
        rel = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="py_scratch"
        )
        assert project.run_sql(f"select count(*) from {rel}", fetch="one")[0] == 3
        assert (
            project.run_sql(f"select count(*) from {rel} where f is null", fetch="one")[0]
            == 1
        )
        assert (
            project.run_sql(f"select count(*) from {rel} where s is null", fetch="one")[0]
            == 1
        )

    def test_python_on_python_chain(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        rel = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="py_chain"
        )
        mismatches = project.run_sql(
            f"select count(*) from {rel} where amount_x2 <> amount * 2", fetch="one"
        )[0]
        assert mismatches == 0

    def test_sql_on_python_mixed_dag(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        check_relations_equal(project.adapter, ["sql_on_py", "py_ref"])

    def test_source_model(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "-s", "py_source"])
        rel = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema, identifier="py_source"
        )
        assert project.run_sql(f"select count(*) from {rel}", fetch="one")[0] == 6


class TestPythonIncrementalNotSupported:
    """MVP guard: the incremental materialization does not support python.

    dbt returns a failed run result (the node's status is 'error') — it does not
    raise — because the teradata incremental materialization does not declare
    supported_languages=['sql','python'].
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"orders_seed.csv": orders_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": upstream_sql,
            "py_incremental.py": py_incremental_model,
        }

    def test_incremental_python_is_rejected(self, project):
        run_dbt(["seed"])
        # Unsupported language -> the py_incremental node fails with a clear
        # "only supports languages ['sql']" error (dbt returns a failed result,
        # it does not raise). Lock that behaviour.
        results = run_dbt(["run"], expect_pass=False)
        statuses = {r.node.name: r.status for r in results}
        assert statuses.get("py_incremental") == "error"


# --------------------------------------------------------------------------- #
# Write-back behaviour: empty, fastload, idempotent re-run, atomic durability
# (plan cases B-07, B-08, B-10, D-04)
# --------------------------------------------------------------------------- #
py_empty_model = """
import pandas as pd

def model(dbt, session):
    dbt.config(materialized='table')
    return pd.DataFrame({'id': pd.Series([], dtype='int64'),
                         'name': pd.Series([], dtype='object')})
"""

py_fastload_model = """
import pandas as pd

def model(dbt, session):
    dbt.config(materialized='table', use_fastload=True)
    n = 3000  # substantial row count for the FastLoad path (single executemany into an empty table)
    return pd.DataFrame({'n': list(range(1, n + 1)), 'sq': [i * i for i in range(1, n + 1)]})
"""

# A >32000-char value must be stored as CLOB (NO PRIMARY INDEX), not a capped VARCHAR.
py_clob_model = """
import pandas as pd

def model(dbt, session):
    dbt.config(materialized='table')
    return pd.DataFrame({'big': ['x' * 40000, 'y' * 10]})
"""

# Builds a real target from the seed; reused to prove re-run idempotency and
# that a failure during the swap leaves the previous target intact.
py_durable_model = """
def model(dbt, session):
    dbt.config(materialized='table')
    return dbt.ref('upstream')
"""


class TestPythonModelWriteBack:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"orders_seed.csv": orders_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": upstream_sql,
            "py_empty.py": py_empty_model,
            "py_fastload.py": py_fastload_model,
            "py_clob.py": py_clob_model,
            "py_durable.py": py_durable_model,
        }

    def _rel(self, project, identifier):
        return project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier=identifier,
        )

    def test_empty_dataframe_creates_zero_row_table(self, project):
        """B-07: an empty DataFrame yields a schema-only table (0 rows)."""
        run_dbt(["seed"])
        run_dbt(["run", "-s", "py_empty"])
        rel = self._rel(project, "py_empty")
        assert project.run_sql(f"select count(*) from {rel}", fetch="one")[0] == 0
        # columns exist and are selectable
        project.run_sql(f"select id, name from {rel}", fetch="all")

    def test_fastload_write_back(self, project):
        """B-08: use_fastload=True loads all rows correctly across >1 batch."""
        run_dbt(["seed"])
        run_dbt(["run", "-s", "py_fastload"])
        rel = self._rel(project, "py_fastload")
        assert project.run_sql(f"select count(*) from {rel}", fetch="one")[0] == 3000
        assert project.run_sql(f"select max(sq) from {rel}", fetch="one")[0] == 9000000

    def test_clob_write_back(self, project):
        """C-08: a >32000-char string is stored as CLOB (NO PRIMARY INDEX)."""
        run_dbt(["seed"])
        run_dbt(["run", "-s", "py_clob"])
        rel = self._rel(project, "py_clob")
        assert project.run_sql(f"select count(*) from {rel}", fetch="one")[0] == 2
        # a 40000-char value only fits in a CLOB (VARCHAR caps at 32000)
        assert project.run_sql(
            f"select max(char_length(big)) from {rel}", fetch="one"
        )[0] == 40000

    def test_reruns_are_idempotent(self, project):
        """B-10: a second run rebuilds in place and leaves no staging table."""
        run_dbt(["seed"])
        run_dbt(["run", "-s", "+py_durable"])
        rel = self._rel(project, "py_durable")
        first = project.run_sql(f"select count(*) from {rel}", fetch="one")[0]
        run_dbt(["run", "-s", "+py_durable"])
        second = project.run_sql(f"select count(*) from {rel}", fetch="one")[0]
        assert first == second and first > 0
        # no orphaned __dbt_tmp staging table remains
        staging = project.run_sql(
            "select count(*) from dbc.tablesv "
            f"where upper(databasename) = upper('{project.test_schema}') "
            "and upper(tablename) = upper('py_durable__dbt_tmp')",
            fetch="one",
        )[0]
        assert staging == 0

    def test_target_intact_on_write_failure(self, project, monkeypatch):
        """D-04: a failure during the swap leaves the previous target intact."""
        run_dbt(["seed"])
        run_dbt(["run", "-s", "+py_durable"])
        rel = self._rel(project, "py_durable")
        before = project.run_sql(f"select count(*) from {rel}", fetch="one")[0]
        assert before > 0

        from dbt.adapters.teradata import python_submissions as ps

        original_drop = ps.TeradataClientPythonJobHelper._drop_if_exists

        def flaky_drop(cursor, target_relation):
            # Fail only when dropping the REAL target during the swap — not the
            # staging table — simulating a mid-swap failure.
            if target_relation.rstrip('"').endswith("py_durable"):
                raise RuntimeError("injected swap failure")
            return original_drop(cursor, target_relation)

        monkeypatch.setattr(
            ps.TeradataClientPythonJobHelper,
            "_drop_if_exists",
            staticmethod(flaky_drop),
        )
        results = run_dbt(["run", "-s", "py_durable"], expect_pass=False)
        assert any(r.status == "error" for r in results)

        # the previous target survived, unchanged
        after = project.run_sql(f"select count(*) from {rel}", fetch="one")[0]
        assert after == before


# --------------------------------------------------------------------------- #
# Guardrails: unsupported materializations / OTF / bad return type
# (plan cases F-02, F-03, E-05)
# --------------------------------------------------------------------------- #
py_view_model = """
import pandas as pd

def model(dbt, session):
    dbt.config(materialized='view')
    return pd.DataFrame({'x': [1, 2, 3]})
"""

py_badreturn_model = """
def model(dbt, session):
    dbt.config(materialized='table')
    return [1, 2, 3]          # not a DataFrame
"""


class TestPythonModelGuards:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "py_view.py": py_view_model,
            "py_badreturn.py": py_badreturn_model,
        }

    def test_view_python_is_rejected(self, project):
        """F-02: view materialization does not support python."""
        results = run_dbt(["run", "-s", "py_view"], expect_pass=False)
        statuses = {r.node.name: r.status for r in results}
        assert statuses.get("py_view") == "error"

    def test_bad_return_type_errors(self, project):
        """E-05: returning a non-DataFrame fails cleanly."""
        results = run_dbt(["run", "-s", "py_badreturn"], expect_pass=False)
        statuses = {r.node.name: r.status for r in results}
        assert statuses.get("py_badreturn") == "error"
