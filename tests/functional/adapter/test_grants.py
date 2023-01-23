import pytest
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


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
    pass

