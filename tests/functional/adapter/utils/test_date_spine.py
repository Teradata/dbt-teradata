import pytest
from dbt.tests.util import run_dbt , check_relation_types,relation_from_name

report_sql="""
        {{
          config(
            materialized="table"
          )
        }}
        {{ date_spine(
            datepart="day",
            start_date="cast('2019-01-01' as date)",
            end_date="cast('2021-01-01' as date)"
          )
        }}
"""

class Test_date_spine:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "project_for_test",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "report.sql": report_sql
        }
    
    def test_validate_date_spine(self,project):
        result=run_dbt(["run"])
        assert len(result) == 1

        result_statuses = sorted(r.status for r in result)
        print(result_statuses)
        assert result_statuses == ["success"]

        relation=relation_from_name(project.adapter,"report")
        no_of_rows=project.run_sql(f"select count(*) as num_rows from {relation}",fetch="one")
        assert relation.identifier=="report"
        assert no_of_rows[0]==731
        

        


