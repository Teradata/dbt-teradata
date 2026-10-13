import pytest

from dbt.tests.adapter.simple_copy.test_simple_copy import (
    SimpleCopyBase,
    EmptyModelsArentRunBase,
)
from dbt.tests.adapter.simple_copy.test_copy_uppercase import BaseSimpleCopyUppercase


class TestSimpleCopyTeradata(SimpleCopyBase):
    def test_simple_copy_with_materialized_views(self, project):
        pytest.skip(
            "Teradata has no standard CREATE MATERIALIZED VIEW syntax; "
            "join indexes are the Teradata equivalent but are not equivalent DDL."
        )


@pytest.mark.skip(
    reason=(
        "EmptyModelsArentRunBase.test_dbt_doesnt_run_empty_models calls "
        "project.get_tables_in_schema() which queries information_schema.tables "
        "using ILIKE — a PostgreSQL-specific operator not supported in Teradata (Error 3707)."
    )
)
class TestEmptyModelsArentRunTeradata(EmptyModelsArentRunBase):
    pass


@pytest.mark.skip(reason="BaseSimpleCopyUppercase requires dbt-postgres adapter which is not installed")
class TestSimpleCopyUppercaseTeradata(BaseSimpleCopyUppercase):
    pass
