import pytest
from pathlib import Path
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    test_table_csv,
    table_with_cte_sql,
    table_from_source_for_catalog_with_schema_change_sql,
    alter_table_add_new_column,
    sources_yml
)

class Test_validate_teradata_on_schema_change:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_teradata_on_schema_change",
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "test_table.csv": test_table_csv
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_from_source_for_catalog_test.sql": table_from_source_for_catalog_with_schema_change_sql,
            "sources.yml": sources_yml
        }

    def test_validate_teradata_cases(self,project):
        result1=run_dbt(["seed"])
        assert len(result1) == 1

        result_statuses = sorted(r.status for r in result1)
        assert result_statuses == ["success"]

        result2=run_dbt(["run"])
        assert len(result2)==1

        relation1=relation_from_name(project.adapter,"table_from_source_for_catalog_test")
        with_cte_result=project.run_sql(f"select * from {relation1}",fetch="all")
        
        # Expecting 3 columns
        assert len(with_cte_result[0])==3

        # Add a new column to the seed table
        project.run_sql(alter_table_add_new_column);

        # Re-run model
        result2=run_dbt(["run"])
        assert len(result2)==1

        relation1=relation_from_name(project.adapter,"table_from_source_for_catalog_test")
        with_cte_result=project.run_sql(f"select * from {relation1}",fetch="all")
        
        # Expecting 4 columns
        assert len(with_cte_result[0])==4
