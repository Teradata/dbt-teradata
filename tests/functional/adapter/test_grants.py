import pytest
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants

my_snapshot_sql = """
    {% snapshot my_snapshot %}
        {{ config(
            check_cols='all', unique_key='id', strategy='check',
            target_database=database, target_schema=schema
        ) }}
        select 1 as id, cast('blue' as varchar(100)) as color
    {% endsnapshot %}
    """.strip()

snapshot_schema_yml = """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""


class TestInvalidGrantsTeradata(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "[Teradata Database] [Error 3802]"

    def privilege_does_not_exist_error(self):
        return "[Teradata Database] [Error 3706]"
    pass

class TestModelGrantsTeradata(BaseModelGrants):
    pass

class TestIncrementalGrantsTeradata(BaseIncrementalGrants):
    pass

class TestSeedGrantsTeradata(BaseSeedGrants):
    pass

class TestSnapshotGrantsTeradata(BaseSnapshotGrants):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_snapshot.sql": my_snapshot_sql,
            "schema.yml": self.interpolate_name_overrides(snapshot_schema_yml),
        }
    pass
