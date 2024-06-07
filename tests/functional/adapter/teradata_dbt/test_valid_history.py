import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture, relation_from_name

from tests.functional.adapter.teradata_dbt.teradata_fixtures import(
    valid_history_target_table_sql,
)

valid_history_sql = """
{{
    config(
        materialized='incremental',
        unique_key='pk',
        incremental_strategy='valid_history',
        valid_from='valid_from',
        history_column_in_target = 'valid_per',
    )
}}
select * from {{ ref('history_soruce') }}
"""

history_soruce_csv_1 = """
pk,valid_from,value_txt1,value_txt2
1,2024-03-01 00:00:00.0000,A,x1
1,2024-03-12 00:00:00.0000,B,x1
1,2024-03-12 00:00:00.0000,B,x2
1,2024-03-25 00:00:00.0000,A,x2
2,2024-03-01 00:00:00.0000,A,x1
2,2024-03-12 00:00:00.0000,C,x1
2,2024-03-12 00:00:00.0000,D,x1
2,2024-03-13 00:00:00.0000,C,x1
2,2024-03-14 00:00:00.0000,C,x1
""".lstrip()

# history_soruce_csv_2 = """
# pk,valid_from,value_txt1,value_txt2
# 1,2024-04-05 00:00:00.0000,D,x2
# 1,2024-03-15 00:00:00.0000,X,x2
# 2,2024-04-05 00:00:00.0000,C,x2
# """.lstrip()

seed_properties_yaml="""
version: 2
seeds:
  - name: history_soruce
    config:
      column_types:
        pk: integer
        valid_from: timestamp
        value_txt1: varchar(1000)
        value_txt2: varchar(1000)
"""

class Test_valid_history:

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(valid_history_target_table_sql)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_valid_history",
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "history_soruce.csv": history_soruce_csv_1,
            "properties.yml": seed_properties_yaml
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "valid_history.sql": valid_history_sql,
        }

    def test_valid_history(self, project):

        result1 = run_dbt(["seed"])
        assert len(result1) == 1

        results2 = run_dbt(["run"])
        assert len(results2) == 1
