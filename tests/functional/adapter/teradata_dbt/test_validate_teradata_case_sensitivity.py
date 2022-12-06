import pytest
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    table_for_case_sensitivity_sql
)

class Test_validate_teradata_case_sensitivity:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_teradata_case_sentivity",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_for_case_sensitivity.sql": table_for_case_sensitivity_sql
        }

    def test_validate_teradata_cases_sentivity(self,project):
        result=run_dbt(["run"])
        assert len(result)==1

        result_statuses = sorted(r.status for r in result)
        assert result_statuses == ["success"]