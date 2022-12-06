import pytest
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name,check_result_nodes_by_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    test_table_csv,
    table_from_source_for_catalog_test_sql,
    view_from_source_for_catalog_test_sql,
    sources_yml
)

class Test_validate_teradata_catalog:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "validate_teradata_testcases"

        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "test_table.csv": test_table_csv
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_from_source_for_catalog_test.sql": table_from_source_for_catalog_test_sql,
            "view_from_source_for_catalog_test.sql": view_from_source_for_catalog_test_sql,
            "sources.yml": sources_yml
        }

    def test_validate_teradata_catalog(self,project):
        result1=run_dbt(["seed"])
        assert len(result1) == 1

        result_statuses = sorted(r.status for r in result1)
        assert result_statuses == ["success"]

        result2=run_dbt(["run"])
        assert len(result2)==2

        result_statuses2= sorted(r.status for r in result2)
        assert result_statuses2==["success","success"]

        check_result_nodes_by_name(result2, ["table_from_source_for_catalog_test", "view_from_source_for_catalog_test"])

        catalog=run_dbt(["docs", "generate"])
        assert len(catalog.nodes)==3
        assert len(catalog.sources)==1