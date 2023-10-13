from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.results import RunExecutionResult, RunResult
import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.ephemeral.fixtures import (
    fct_eph_first_sql,
    int_eph_first_sql,
    schema_yml
)



SUPPRESSED_CTE_EXPECTED_OUTPUT = """-- fct_eph_first.sql


with int_eph_first as(
    select * from __dbt__cte__int_eph_first
)

select * from int_eph_first"""


class TestEphemeralCompilation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "int_eph_first.sql": int_eph_first_sql,
            "fct_eph_first.sql": fct_eph_first_sql,
            "schema.yml": schema_yml,
        }
    '''
    def test_ephemeral_compilation(self, project):
        # Note: There are no models that run successfully. This testcase tests running tests.
        results = run_dbt(["run"])
        assert len(results) == 0
    '''
    def test__suppress_injected_ctes(self, project):
        compile_output = run_dbt(
            ["compile", "--no-inject-ephemeral-ctes" , "--select", "fct_eph_first"]
        )
        assert isinstance(compile_output, RunExecutionResult)
        node_result = compile_output.results[0]
        assert isinstance(node_result, RunResult)
        node = node_result.node
        assert isinstance(node, ModelNode)
        assert node.compiled_code == SUPPRESSED_CTE_EXPECTED_OUTPUT