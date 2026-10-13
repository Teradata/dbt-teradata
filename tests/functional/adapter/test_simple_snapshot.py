import pytest

from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshot,
    BaseSimpleSnapshotBase,
    BaseSnapshotCheck,
)
from dbt.tests.adapter.simple_snapshot.test_various_configs import (
    BaseSnapshotColumnNames,
    BaseSnapshotColumnNamesFromDbtProject,
    BaseSnapshotDbtValidToCurrent,
    BaseSnapshotInvalidColumnNames,
    BaseSnapshotMultiUniqueKey,
)
from dbt.tests.adapter.simple_snapshot.test_ephemeral_snapshot_hard_deletes import (
    BaseSnapshotEphemeralHardDeletes,
    BaseSnapshotNewColumnSpecificCheckCols,
    BaseSnapshotNewColumnTimestampStrategy,
    BaseSnapshotNewColumnWithDeletes,
)

# BaseSimpleSnapshot and BaseSnapshotCheck use clone_table() which generates
# "drop table if exists tablename" (PostgreSQL prefix form). Teradata requires suffix form. Error 3707.
_SKIP_CLONE = "setUp uses PostgreSQL-style 'DROP TABLE IF EXISTS' prefix syntax; Teradata requires suffix form (Error 3707)"

# The various_configs tests call project.run_sql(create_snapshot_expected_sql) which uses
# TEXT data type (Error 3706) and md5(...::text) PostgreSQL cast syntax (Error 3707).
_SKIP_TEXT = "setUp SQL uses TEXT type (Error 3706) and ::text cast syntax (Error 3707) unsupported in Teradata"

# The ephemeral/new-column tests call project.run_sql with {database}.{schema}.tablename
# (3-part naming) which Teradata does not support (only 2-part database.tablename).
# Also uses ADD COLUMN syntax that differs from Teradata's ADD columnname type.
_SKIP_3PART = "setUp SQL uses {database}.{schema}.tablename 3-part naming unsupported in Teradata"


@pytest.mark.skip(reason=_SKIP_CLONE)
class TestSimpleSnapshotTeradata(BaseSimpleSnapshot):
    pass


class TestSimpleSnapshotBaseTeradata(BaseSimpleSnapshotBase):
    pass


@pytest.mark.skip(reason=_SKIP_CLONE)
class TestSnapshotCheckTeradata(BaseSnapshotCheck):
    pass


@pytest.mark.skip(reason=_SKIP_TEXT)
class TestSnapshotColumnNamesTeradata(BaseSnapshotColumnNames):
    pass


@pytest.mark.skip(reason=_SKIP_TEXT)
class TestSnapshotColumnNamesFromDbtProjectTeradata(BaseSnapshotColumnNamesFromDbtProject):
    pass


@pytest.mark.skip(reason=_SKIP_TEXT)
class TestSnapshotDbtValidToCurrentTeradata(BaseSnapshotDbtValidToCurrent):
    pass


@pytest.mark.skip(reason=_SKIP_TEXT)
class TestSnapshotInvalidColumnNamesTeradata(BaseSnapshotInvalidColumnNames):
    pass


@pytest.mark.skip(reason=_SKIP_TEXT)
class TestSnapshotMultiUniqueKeyTeradata(BaseSnapshotMultiUniqueKey):
    pass


@pytest.mark.skip(reason=_SKIP_3PART)
class TestSnapshotEphemeralHardDeletesTeradata(BaseSnapshotEphemeralHardDeletes):
    pass


@pytest.mark.skip(reason=_SKIP_3PART)
class TestSnapshotNewColumnSpecificCheckColsTeradata(BaseSnapshotNewColumnSpecificCheckCols):
    pass


@pytest.mark.skip(reason=_SKIP_3PART)
class TestSnapshotNewColumnTimestampStrategyTeradata(BaseSnapshotNewColumnTimestampStrategy):
    pass


@pytest.mark.skip(reason=_SKIP_3PART)
class TestSnapshotNewColumnWithDeletesTeradata(BaseSnapshotNewColumnWithDeletes):
    pass
