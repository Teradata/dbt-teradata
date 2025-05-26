import pytest
from dbt.tests.util import run_dbt, check_result_nodes_by_name

seed_csv="""
id,attrA,attrB,create_date
1,val1A,val1B,2020-03-05
2,val2A,val2B,2020-04-05
3,val3A,val3B,2020-05-05
4,val4A,val4B,2020-10-05
""".lstrip()

view_from_source_for_catalog_test_sql="""
        {{
            config(
                materialized="view"
            )
        }}
        SELECT * FROM {{ ref('seed') }}
"""

table_from_source_for_catalog_test_sql="""
        {{
            config(
                materialized="table"
            )
        }}
    SELECT * FROM {{ ref('seed') }}
"""


class Test_fallback_schema:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_fallback_schema",
            "vars": {
                    "fallback_schema": "HASH_TEST"
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_from_source_for_catalog_test.sql": table_from_source_for_catalog_test_sql,
            "view_from_source_for_catalog_test.sql": view_from_source_for_catalog_test_sql
        }


    def test_fallback_schema(self, project):

        result1 = run_dbt(["seed"])
        assert len(result1) == 1

        result2 = run_dbt(["run"])
        assert len(result2) == 2

        check_result_nodes_by_name(result2, ["table_from_source_for_catalog_test", "view_from_source_for_catalog_test"])

        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1
