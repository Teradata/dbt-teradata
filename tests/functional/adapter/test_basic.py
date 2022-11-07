import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


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


class TestGenericTestsTeradata(BaseGenericTests):
    pass


class TestSnapshotCheckColsTeradata(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampTeradata(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass
