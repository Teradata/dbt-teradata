import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture, relation_from_name

valid_history_sql = """
{{
    config(
        materialized='incremental',
        unique_key='pk',
        incremental_strategy='valid_history',
        valid_from='valid_from',
        use_valid_to_time='no',
        resolve_conflicts='yes',
        history_column_in_target = 'valid_per',
    )
}}
select * from {{ ref('history_soruce') }}
"""

history_soruce_csv_1 = """
pk,valid_from,value_txt1,value_txt2z
1,2024-03-01,A,x1
1,2024-03-12,B,x1
1,2024-03-12,B,x2
1,2024-03-25,A,x2
2,2024-03-01,A,x1
2,2024-03-12,C,x1
2,2024-03-12,D,x1
2,2024-03-13,C,x1
2,2024-03-14,C,x1
""".lstrip()

history_soruce_csv_2 = """
pk,valid_from,value_txt1,value_txt2
1,2024-04-05,D,x2
1,2024-03-15,X,x2
2,2024-04-05,C,x2
""".lstrip()


class Test_valid_history:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_valid_history",
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "history_soruce.csv": history_soruce_csv_1,
            "history_soruce.csv": history_soruce_csv_2
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "valid_history.sql": valid_history_sql

        }

    def test_valid_history(self, project):

        result1 = run_dbt(["seed"])
        assert len(result1) == 1

        results2 = run_dbt(["run"])
        assert len(results2) == 1

        relation1 = relation_from_name(project.adapter, "valid_history")
        with_cte_result = project.run_sql(f"select * from {relation1}", fetch="all")

        results3 = run_dbt(["run"])
        assert len(results3) == 1