import pytest
from dbt.tests.util import run_dbt 

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    test_table_for_type_inference_csv
    )

class Test_validate_teradata_seed_type_inference:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_teradata_seed_type_inference"
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "test_table_for_type_inference.csv": test_table_for_type_inference_csv   
        }

    def test_validate_teradata_seed_type_inference(self,project):
        result=run_dbt(["seed"])
        assert len(result)==1

        result_statuses = sorted(r.status for r in result)
        assert result_statuses == ["success"]