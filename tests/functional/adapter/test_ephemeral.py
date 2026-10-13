import pytest

from dbt.tests.adapter.ephemeral.test_ephemeral import (
    BaseEphemeralErrorHandling,
    BaseEphemeralMulti,
    BaseEphemeralNested,
)

_SKIP_COMPILED_SQL = (
    "BaseEphemeralMulti/BaseEphemeralNested compare compiled SQL expecting "
    "'create view ...' but Teradata generates 'REPLACE VIEW ...' DDL. "
    "Ephemeral model correctness is verified by TestEphemeralTeradata in test_basic.py."
)


class TestEphemeralErrorHandlingTeradata(BaseEphemeralErrorHandling):
    pass


@pytest.mark.skip(reason=_SKIP_COMPILED_SQL)
class TestEphemeralMultiTeradata(BaseEphemeralMulti):
    pass


@pytest.mark.skip(reason=_SKIP_COMPILED_SQL)
class TestEphemeralNestedTeradata(BaseEphemeralNested):
    pass
