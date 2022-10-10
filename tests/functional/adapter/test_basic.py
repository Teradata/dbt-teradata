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

'''
class TestSimpleMaterializationsMyAdapter(BaseSimpleMaterializations):
    pass


class TestSingularTestsMyAdapter(BaseSingularTests):
    print("1st testcase")
    pass


class TestSingularTestsEphemeralMyAdapter(BaseSingularTestsEphemeral):
    print("second testcase")
    pass


class TestEmptyMyAdapter(BaseEmpty):
    print("Third testcase")
    pass


class TestEphemeralMyAdapter(BaseEphemeral):
    print("fourth testcase")
    pass


class TestIncrementalMyAdapter(BaseIncremental):
    print("Fifth testcase")
    pass


class TestGenericTestsMyAdapter(BaseGenericTests):
    print("Sixth testcase")
    pass

'''
class TestSnapshotCheckColsMyAdapter(BaseSnapshotCheckCols):
    print("Seventh testcase")
    pass

'''
class TestSnapshotTimestampMyAdapter(BaseSnapshotTimestamp):
    print("Eighth testcase")
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    print("Ninth testcase")
    pass
'''