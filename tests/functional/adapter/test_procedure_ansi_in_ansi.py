import pytest
from dbt.tests.util import run_dbt

macro_calling_procedure = """
{% macro run_proc() %}
    {% do run_query("CALL dummy_test_tmode.CurrencyConversionProcedureANSI(100.00, 'USD', 'EUR');") %} 
{% endmacro %}
"""

class TestProcedureANSIInANSI:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro_calling_procedure.sql": macro_calling_procedure}

    def test_procedure_ansi_in_ansi(
            self,
            project,
    ):
         run_dbt(["run-operation", "run_proc"], expect_pass=True)
