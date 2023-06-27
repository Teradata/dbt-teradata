import pytest

from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates

models__delete_insert_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

{% if not is_incremental() %}

select * from (select 1 as id, 'goodbye' as msg, 'purple' as color) as "dual"
union all
select * from (select 2 as id, 'anyway' as msg, 'red' as color) as "dual"
union all
select * from (select 3 as id, 'hey' as msg, 'blue' as color) as "dual"

{% else %}

-- delete will not happen on the above record where id = 2, so new record will be inserted instead
select * from (select 1 as id, 'goodbye' as msg, 'purple' as color) as "dual"
union all
select * from (select 2 as id, 'yo' as msg, 'green' as color) as "dual"
union all
select * from (select 3 as id, 'hi' as msg, 'blue' as color) as "dual"

{% endif %}
"""

seeds__expected_delete_insert_incremental_predicates_csv = """id,msg,color
1,goodbye,purple
2,anyway,red
2,yo,green
3,hi,blue
"""

class TestIncrementalPredicatesDeleteInsertTeradata(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__delete_insert_incremental_predicates_sql
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_delete_insert_incremental_predicates.csv": seeds__expected_delete_insert_incremental_predicates_csv
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": [
                    "id <> 2"
                ],
                "+incremental_strategy": "delete+insert"
            }
        }
    pass


class TestPredicatesDeleteInsertTeradata(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__delete_insert_incremental_predicates_sql
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_delete_insert_incremental_predicates.csv": seeds__expected_delete_insert_incremental_predicates_csv
        }
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": [
                    "id <> 2"
                ],
                "+incremental_strategy": "delete+insert"
            }
        }

