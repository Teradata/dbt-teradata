import pytest
from dbt.tests.util import run_dbt, check_result_nodes_by_name
import uuid

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    test_table_csv,
    table_from_source_for_catalog_test_sql,
    view_from_source_for_catalog_test_sql,
    sources_yml
)



class Test_fallback_schema:

    @pytest.fixture(scope="class")
    def random_db_name(self):
        return f"tests_{uuid.uuid4().hex[:8]}"

    @pytest.fixture(scope="class")
    def project_config_update(self, random_db_name):
        return {
            "name": "test_fallback_schema",
            "vars": {
                    "fallback_schema": random_db_name
            }
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


    def test_fallback_schema(self, project, random_db_name):

        project.run_sql(f"create database {random_db_name} as perm=100000;")

        result1 = run_dbt(["seed"])
        assert len(result1) == 1

        result2 = run_dbt(["run"])
        assert len(result2) == 2

        check_result_nodes_by_name(result2, ["table_from_source_for_catalog_test", "view_from_source_for_catalog_test"])

        catalog = run_dbt(["docs", "generate"])
        # assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

        project.run_sql(f"delete database {random_db_name};")
        project.run_sql(f"drop database {random_db_name};")
