import pytest
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    dbcinfo_sql,
    generate_schema_name_sql
)

class Test_validate_teradata_case_sensitivity_with_macro_override:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_teradata_case_sensitivity_with_macro_override",
            "version": "1.1.0",
            "config-version": 2,
            "dispatch":[{"macro_namespace": "dbt","search_order": ['validate_teradata_case_sensitivity_with_macro_override', 'dbt']}]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dbcinfo.sql": dbcinfo_sql
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return{
            "generate_schema_name.sql": generate_schema_name_sql
        }

    def test_validate_teradata_case_sensitivity_with_macro_override(self,project):
        result=run_dbt(["run"])
        assert len(result)==1 

        result_statuses = sorted(r.status for r in result)
        assert result_statuses == ["success"]
