import pytest
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    test_table_csv
)

class Test_validate_teradata_fastload:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_teradata_fastload",
            "seeds":{"+use_fastload": "true"}
        }
    
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "test_table.csv": test_table_csv   
        }

    def test_validate_teradata_fastload(self,project):
        result=run_dbt(["seed"])
        assert len(result)==1

        relation=relation_from_name(project.adapter,"test_table")
        test_table=project.run_sql(f"select count(*) as num_rows from {relation}",fetch="one")
        assert test_table[0]==4