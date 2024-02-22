"""
We will be creating a seed table from 'sample_seed_csv'.
If we look at the 'project_config_update': we can see se are setting the datatype of 'json_column' as JSON.
In 'model_model_sql' model, we will fetch the columns,datatypes and few other fields of sample_seed table with help of
macro 'get_columns_in_relation'.
As we have now added support for JSON datatype, we will retrieve JSON datatype for 'json_column' and will build the
'model_model_sql' model.
Previously it would have returned N/A as datatype for 'json_column' and would have failed to build the
'model_model_sql'
"""

import pytest
from dbt.tests.util import run_dbt

sample_seed_csv = """
id,json_column
1,{"id": 1}
2,{"id": 2}
3,{"id": 3}
4,{"id": 4} """.lstrip()

models__model_sql = """
with source as (
select 
{% set columns = adapter.get_columns_in_relation(ref('sample_seed')) %}
{% for column in columns %}
    cast({{ column.column }} as {{column.dtype}}) as {{ column.column }}
    {%- if not loop.last %}, {% endif %}
{% endfor %}
from {{ ref('sample_seed') }}
)
sel * from source
"""

class Test_columns_in_relation:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "get_columns_in_relation",
            "seeds": {"get_columns_in_relation":
                          {"sample_seed":
                               {"+column_types":
                                    {"json_column": "JSON"}
                                }
                           }
                      }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "sample_seed.csv": sample_seed_csv
        }
    @pytest.fixture(scope="class")
    def models(self):
        return{
            "models__model.sql": models__model_sql,
        }

    def test_get_columns_in_relation(self, project):
        results=run_dbt(["seed"])
        assert len(results) == 1

        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ['success']

        result2 = run_dbt(["run"])
        assert len(result2) == 1

        result_statuses = sorted(r.status for r in result2)
        assert result_statuses == ['success']