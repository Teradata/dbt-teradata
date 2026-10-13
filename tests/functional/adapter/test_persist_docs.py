import pytest

from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsAllColumnsMissing,
    BasePersistDocsBase,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
    BasePersistDocsQuotedColumnCaseSensitive,
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch,
)


class TestPersistDocsTeradata(BasePersistDocs):
    def test_has_comments_pglike(self, project):
        # Teradata's COMMENT ON TABLE/COLUMN string is capped at 255 characters
        # (DBC.TablesV/ColumnsV CommentString is CHAR(255)); anything past that is
        # silently truncated by the database. The upstream fixture's description
        # is 1000+ characters (to exercise quoting of special characters), so the
        # persisted comment is legitimately truncated to 255 chars on Teradata --
        # this is a platform limit, not an adapter bug.
        pytest.skip(
            "Teradata truncates COMMENT ON TABLE/COLUMN text to 255 characters "
            "(DBC CommentString is CHAR(255)); the fixture's long description "
            "cannot round-trip in full."
        )


class TestPersistDocsBaseTeradata(BasePersistDocsBase):
    pass


class TestPersistDocsColumnMissingTeradata(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsAllColumnsMissingTeradata(BasePersistDocsAllColumnsMissing):
    pass


class TestPersistDocsCommentOnQuotedColumnTeradata(BasePersistDocsCommentOnQuotedColumn):
    pass


class TestPersistDocsQuotedColumnCaseSensitiveTeradata(BasePersistDocsQuotedColumnCaseSensitive):
    pass


class TestPersistDocsQuotedDescriptionNotAppliedOnMismatchTeradata(BasePersistDocsQuotedDescriptionNotAppliedOnMismatch):
    pass
