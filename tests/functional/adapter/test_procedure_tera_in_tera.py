import pytest
from dbt.tests.util import run_dbt

macro_calling_procedure = """
{% macro run_proc() %}
    {% do run_query("CALL CurrencyConversionProcedureTERA(100.00, 'USD', 'EUR');") %} 
{% endmacro %}
"""

class TestProcedureTERAInTERA:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro_calling_procedure.sql": macro_calling_procedure}

    def test_procedure_tera_in_tera(
            self,
            project,
    ):
        run_dbt(["run-operation", "run_proc"], expect_pass=True)
