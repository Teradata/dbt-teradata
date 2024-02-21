import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture, relation_from_name

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

# generate_column_names_sql="""
# {% macro generate_column_names(model) -%}
#     {% set columns = adapter.get_columns_in_relation(ref('model')) %}
#     {{ return(columns) }}
# {%- endmacro %}
# """

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

    # @pytest.fixture(scope="class")
    # def macros(self):
    #     return {
    #         "generate_column_names.sql": generate_column_names_sql
    #     }

    def test_get_columns_in_relation(self, project):
        results=run_dbt(["seed"])
        assert len(results) == 1

        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ['success']

        result2 = run_dbt(["run"])
        assert len(result2) == 1

        result_statuses = sorted(r.status for r in result2)
        assert result_statuses == ['success']