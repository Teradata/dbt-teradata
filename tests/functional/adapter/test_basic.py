import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental,BaseIncrementalNotSchemaChange
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


config_materialized_incremental_append = """
  {{ config(materialized="incremental", incremental_strategy="append") }}
"""
model_incremental = """
select * from {{ source('raw', 'seed') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

incremental_append_sql = config_materialized_incremental_append + model_incremental

config_materialized_incremental_delete_insert = """
  {{ config(materialized="incremental", incremental_strategy="delete+insert") }}
"""
model_incremental = """
select * from {{ source('raw', 'seed') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

incremental_delete_insert_sql = config_materialized_incremental_delete_insert + model_incremental

schema_base_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
"""

incremental_not_schema_change_sql = """
{{ config(materialized="incremental", unique_key="user_id_current_time",on_schema_change="sync_all_columns") }}
select
    Concat('1','-',cast(current_timestamp as varchar(40))) as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""


class TestSimpleMaterializationsTeradata(BaseSimpleMaterializations):
    pass


class TestSingularTestsMyTeradata(BaseSingularTests):
    pass

'''
class TestSingularTestsEphemeralTeradata(BaseSingularTestsEphemeral):
    pass

# FAIL
  # test_dbt_ephemeral_data_tests: data_test_ephemeral_models - fails with
  #   [Error 6926] WITH [RECURSIVE] clause or recursive view is not supported
  #   within WITH [RECURSIVE] definitions, views, triggers or stored procedures.
'''
class TestEmptyTeradata(BaseEmpty):
    pass


class TestEphemeralTeradata(BaseEphemeral):
    pass

class TestIncrementalTeradata(BaseIncremental):
    pass

class TestIncrementalAppendTeradata(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_append_sql, "schema.yml": schema_base_yml}
    
    pass

class TestIncrementalDeleteInsertTeradata(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_delete_insert_sql, "schema.yml": schema_base_yml}
    
    pass


class TestBaseIncrementalNotSchemaChangeTeradata(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}
    pass

class TestGenericTestsTeradata(BaseGenericTests):
    pass


class TestSnapshotCheckColsTeradata(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampTeradata(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass
