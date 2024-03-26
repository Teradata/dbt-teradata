import pytest
from dbt.tests.util import run_dbt
import os
import json
seed_csv = """
id,name
1,user1
2,user2
3,user3
4,user4
""".lstrip()

model_sql = """
{{
config( 
    materialized = 'incremental', 
    incremental_strategy = 'merge',
    unique_key = 'id' 
)
}}
sel * from {{ ref('seed') }}
"""

class Test_run_results_json:

    def project_config_update(self):
        return {
            "name": "get_columns_in_relation"
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
        }

    def test_run_results_json(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        run_results_path = os.path.join(project.project_root, "target", "run_results.json")
        path = './target/run_results.json'
        run_results = open(path)
        run_results_json = json.load(run_results)
        rows_affected = run_results_json["results"][0]["adapter_response"]["rows_affected"]
        assert rows_affected == 4
        message = run_results_json["results"][0]["adapter_response"]["code"]
        assert message == "Insert"

        run_dbt(["run"])
        path = './target/run_results.json'
        run_results = open(path)
        run_results_json = json.load(run_results)
        message = run_results_json["results"][0]["adapter_response"]["code"]
        assert message == "Merge Update"
        rows_affected = run_results_json["results"][0]["adapter_response"]["rows_affected"]
        assert rows_affected == 4

