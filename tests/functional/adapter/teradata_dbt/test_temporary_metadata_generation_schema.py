import pytest
from dbt.tests.util import run_dbt, read_file
import pathlib
import re

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
                materialized="incremental"
            )
        }}
        
        {% set view_relation = ref('view_from_source_for_catalog_test') %}
        {% set view_columns = adapter.get_columns_in_relation(view_relation) %}

        SELECT 
    {{ view_columns|length }} as column_count,
    CURRENT_TIMESTAMP as test_timestamp
"""


class Test_temporary_metadata_generation_schema:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_temporary_metadata_generation_schema",
            "vars": {
                    "temporary_metadata_generation_schema": "HASH_TEST"
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


    def test_temporary_metadata_generation_schema(self, project):

        result1 = run_dbt(["seed"])
        assert len(result1) == 1

        (pathlib.Path(project.project_root) / "log_output").mkdir(parents=True, exist_ok=True)
        run_dbt(["--log-path", "log_output","run"])
        log_output = read_file("log_output", "dbt.log").replace("\n", " ").replace("\\n", " ")
        pattern = r'"HASH_TEST"\."view_from_source_for_catalog_test_tmp_viw_tbl_\d+"'
        matches = re.findall(pattern, log_output)
        # assert '"HASH_TEST".' in log_output
        assert matches, "No temp_table with random suffix found in dbt run log file!"

        (pathlib.Path(project.project_root) / "log_output_catalog").mkdir(parents=True, exist_ok=True)
        catalog = run_dbt(["--log-path", "log_output_catalog","docs", "generate"])
        assert len(catalog.nodes) == 3
        log_output_catalog = read_file("log_output_catalog", "dbt.log").replace("\n", " ").replace("\\n", " ")
        pattern = r'"HASH_TEST"\."view_from_source_for_catalog_test_tmp_viw_tbl_\d+"'
        matches = re.findall(pattern, log_output_catalog)
        assert matches, "No temp_table with random suffix found in catalog log file!"