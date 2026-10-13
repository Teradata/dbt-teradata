import pytest
from dbt.tests.adapter.empty.test_empty import BaseTestEmpty, BaseTestEmptySeedFlag, BaseTestEmptyInlineSourceRef
from dbt.tests.util import relation_from_name, run_dbt


class TestEmptyTeradata(BaseTestEmpty):
    pass


class TestEmptySeedFlagTeradata(BaseTestEmptySeedFlag):
    pass


@pytest.mark.skip(
    reason=(
        "render_limited() appends '_dbt_limit_subq' alias; when the Jinja template also "
        "adds 'as raw_source', Teradata rejects the double alias (Error 3706). "
        "The fix requires dbt-core changes to how render_limited() interacts with "
        "explicit FROM-clause aliases in Jinja templates."
    )
)
class TestEmptyInlineSourceRefTeradata(BaseTestEmptyInlineSourceRef):
    pass


_SEED_CSV = """\
id,name,value
1,Alice,1.23
2,Bob,99.99
"""


class _BaseEmptySeedDdlConfig:
    """Shared fixtures and zero-row assertion for --empty with Teradata-specific DDL configs.

    Subclasses only need to override project_config_update with the seeds DDL config.
    The --empty then --full-refresh round-trip is already covered by TestEmptySeedFlagTeradata.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": _SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def assert_row_count(self, project, relation_name, expected):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == expected

    def test_empty_seed_creates_zero_rows(self, project):
        run_dbt(["seed", "--empty"])
        self.assert_row_count(project, "my_seed", 0)


class TestEmptySeedFlagWithTableKind(_BaseEmptySeedDdlConfig):
    """Validates --empty honors Teradata table_kind (SET/MULTISET) DDL config."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_empty_seed_table_kind",
            "seeds": {
                "table_kind": "SET",
                "index": "UNIQUE PRIMARY INDEX (id)",
            },
        }


class TestEmptySeedFlagWithIndex(_BaseEmptySeedDdlConfig):
    """Validates --empty honors Teradata NO PRIMARY INDEX DDL config."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_empty_seed_index",
            "seeds": {
                "index": "NO PRIMARY INDEX",
            },
        }

