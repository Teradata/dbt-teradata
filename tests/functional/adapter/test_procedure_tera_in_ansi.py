import pytest
from dbt.tests.util import run_dbt_and_capture

macro_calling_procedure = """
{% macro run_proc() %}
    {% do run_query("CALL CurrencyConversionProcedureTERA(100.00, 'USD', 'EUR');") %} 
{% endmacro %}
"""

class TestProcedureTERAInANSI:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro_calling_procedure.sql": macro_calling_procedure}

    def test_procedure_tera_in_ansi(
            self,
            project,
    ):
        _, out = run_dbt_and_capture(["run-operation", "run_proc"], expect_pass=False)
        assert "Invalid session mode for procedure execution" in out
