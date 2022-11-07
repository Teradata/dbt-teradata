import pytest
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    test_table_csv,
    table_from_source_sql,
    sources_yml
)

class Test_validate_teradata_sources:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_teradata_sources",
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "test_table.csv": test_table_csv   
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_from_source.sql": table_from_source_sql,
            "sources.yml": sources_yml
        }

    def test_validate_teradata_sources(self,project):
        # testing for dbt seed
        result1=run_dbt(["seed"])
        assert len(result1) == 1

        result_statuses = sorted(r.status for r in result1)
        assert result_statuses == ["success"]

        relation1=relation_from_name(project.adapter,"test_table")
        no_of_rows=project.run_sql(f"select count(*) as num_rows from {relation1}",fetch="one")
        assert no_of_rows[0]==4
        assert relation1.identifier=="test_table"

        # testing for dbt run
        result2=run_dbt(["run"])
        assert len(result2)==1
        result_statuses2= sorted(r.status for r in result2)
        assert result_statuses2==["success"]

        relation2=relation_from_name(project.adapter,"table_from_source")
        rows=project.run_sql(f"select count(*) as num_rows from {relation2}",fetch="one")
        assert rows[0]==4
        assert relation2.identifier=="table_from_source"

        catalog=run_dbt(["docs", "generate"])
        assert len(catalog.nodes)==2

        #test_dbt_teradata_seed_twice in spec
        second_seed_result=run_dbt(["seed"])
        assert len(second_seed_result)==1

