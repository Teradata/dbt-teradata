import pytest
from dbt.tests.util import run_dbt


from tests.functional.adapter.seed_modifiers.fixtures import(
    sample_seed_csv,
    sample_model_yml
)

class Test_table_modifiers_with_seed_index:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
          "name": "project_table_modifiers_with_seed_index",
          "seeds": {"index": "UNIQUE PRIMARY INDEX (id)"}
        }
    
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "sample_seed.csv": sample_seed_csv
            
        }
    
    def test_table_modifiers_with_seed_index(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
       
        result_statuses = sorted(r.status for r in results)
        assert result_statuses== ['success']

