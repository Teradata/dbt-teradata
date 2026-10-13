import pytest

from dbt.tests.adapter.basic.test_get_catalog_for_single_relation import (
    BaseGetCatalogForSingleRelation,
)
from dbt.tests.util import run_dbt, get_connection
from dbt_common.contracts.metadata import (
    CatalogTable,
    TableMetadata,
    ColumnMetadata,
    StatsItem,
)


class TestGetCatalogForSingleRelationTeradata(BaseGetCatalogForSingleRelation):
    @pytest.fixture(scope="class")
    def expected_catalog_my_seed(self, project):
        columns = {
            "id": ColumnMetadata(type="INTEGER", index=1, name="id", comment=None),
            "first_name": ColumnMetadata(type="CHARACTER VARYING", index=2, name="first_name", comment=None),
            "email": ColumnMetadata(type="CHARACTER VARYING", index=3, name="email", comment=None),
            "ip_address": ColumnMetadata(type="CHARACTER VARYING", index=4, name="ip_address", comment=None),
            "updated_at": ColumnMetadata(type="TIMESTAMP", index=5, name="updated_at", comment=None),
        }
        return CatalogTable(
            metadata=TableMetadata(
                type="table",
                schema=project.test_schema,
                name="my_seed",
                database=None,
                comment=None,
                owner=None,
            ),
            columns=columns,
            stats={
                "has_stats": StatsItem(
                    id="has_stats",
                    label="Has Stats?",
                    value=True,
                    include=False,
                    description="Indicates whether there are statistics for this table",
                ),
            },
        )

    @pytest.fixture(scope="class")
    def expected_catalog_my_view_model(self, project):
        columns = {
            "id": ColumnMetadata(type="INTEGER", index=1, name="id", comment=None),
            "first_name": ColumnMetadata(type="CHARACTER VARYING", index=2, name="first_name", comment=None),
            "email": ColumnMetadata(type="CHARACTER VARYING", index=3, name="email", comment=None),
            "ip_address": ColumnMetadata(type="CHARACTER VARYING", index=4, name="ip_address", comment=None),
            "updated_at": ColumnMetadata(type="TIMESTAMP", index=5, name="updated_at", comment=None),
        }
        return CatalogTable(
            metadata=TableMetadata(
                type="view",
                schema=project.test_schema,
                name="my_view_model",
                database=None,
                comment=None,
                owner=None,
            ),
            columns=columns,
            stats={
                "has_stats": StatsItem(
                    id="has_stats",
                    label="Has Stats?",
                    value=True,
                    include=False,
                    description="Indicates whether there are statistics for this table",
                ),
            },
        )


# --- Additional tests for edge cases ---

models__commented_table_sql = """
{{
    config(
        materialized='table',
        persist_docs={"relation": true, "columns": true},
    )
}}

select
    id,
    first_name
from {{ ref('my_seed') }}
"""

models__commented_table_yml = """
version: 2
models:
  - name: commented_table
    description: "This table has a comment"
    columns:
      - name: id
        description: "Primary key"
      - name: first_name
        description: "The first name"
"""


class TestGetCatalogForSingleRelationWithComments:
    """Validates that table and column comments are returned correctly."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "get_catalog_single_relation_comments"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": "id,first_name\n1,Alice\n",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "commented_table.sql": models__commented_table_sql,
            "schema.yml": models__commented_table_yml,
        }

    def test_catalog_returns_comments(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        relation = project.adapter.get_relation(
            database=project.database,
            schema=project.test_schema,
            identifier="commented_table",
        )

        with get_connection(project.adapter):
            catalog = project.adapter.get_catalog_for_single_relation(relation)

        assert catalog is not None
        assert catalog.metadata.comment == "This table has a comment"
        assert catalog.columns["id"].comment == "Primary key"
        assert catalog.columns["first_name"].comment == "The first name"


class TestGetCatalogForSingleRelationNonExistent:
    """Validates that a non-existent relation returns None."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "get_catalog_single_relation_none"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": "id,first_name\n1,Alice\n",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_nonexistent_relation_returns_none(self, project):
        run_dbt(["seed"])

        # Build a relation object for something that doesn't exist
        relation = project.adapter.Relation.create(
            schema=project.test_schema,
            identifier="this_table_does_not_exist",
        )

        with get_connection(project.adapter):
            catalog = project.adapter.get_catalog_for_single_relation(relation)

        assert catalog is None

models__commented_view_sql = """
{{
    config(
        materialized='view',
        persist_docs={"relation": true, "columns": true},
    )
}}

select
    id,
    first_name
from {{ ref('my_seed') }}
"""

models__commented_view_yml = """
version: 2
models:
  - name: commented_view
    description: "This view has a comment"
    columns:
      - name: id
        description: "View primary key"
      - name: first_name
        description: "The first name in view"
"""


class TestGetCatalogForSingleRelationViewWithComments:
    """Validates that view and view-column comments are returned correctly.

    This exercises the DBC.ColumnsV backfill path for views under use_qvci=False,
    where get_columns_in_relation uses a temp table (losing original comments).
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "get_catalog_single_relation_view_comments"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": "id,first_name\n1,Alice\n",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "commented_view.sql": models__commented_view_sql,
            "schema.yml": models__commented_view_yml,
        }

    def test_catalog_returns_view_comments(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        relation = project.adapter.get_relation(
            database=project.database,
            schema=project.test_schema,
            identifier="commented_view",
        )

        with get_connection(project.adapter):
            catalog = project.adapter.get_catalog_for_single_relation(relation)

        assert catalog is not None
        assert catalog.metadata.type == "view"
        assert catalog.metadata.comment == "This view has a comment"
        assert catalog.columns["id"].comment == "View primary key"
        assert catalog.columns["first_name"].comment == "The first name in view"
