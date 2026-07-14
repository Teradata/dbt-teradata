"""
Functional tests for persist_docs on Teradata (IDE-26225).

Reuses the shared dbt-tests-adapter BasePersistDocs suite (validates relation +
column comments surface in catalog.json, quote/case-sensitivity handling, and
missing-column warnings) and adds Teradata-specific coverage:

  * per-materialization - view / incremental / seed / snapshot comments
  * special characters  - quotes, --, /* */ are escaped and round-trip
  * idempotency         - a re-run with unchanged descriptions issues no COMMENT ON DDL
  * changed description - updated text re-issues the comment DDL
  * truncation          - a >255 char description succeeds (Teradata comment limit)

OTF/Iceberg skip (models with catalog_name set) is handled in teradata__persist_docs
but is not covered here, as it requires an OTF catalog that is not available in this
functional test environment.
"""
import json
import os

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsColumnMissing,
    BasePersistDocsAllColumnsMissing,
    BasePersistDocsQuotedColumnCaseSensitive,
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch,
    BasePersistDocsCommentOnQuotedColumn,
)


# ---------------------------------------------------------------------------
# Shared dbt-tests-adapter suite
#
# NOTE: The flagship BasePersistDocs.test_has_comments_pglike is intentionally
# NOT subclassed. Its shared fixtures produce a `name` column comment of ~280
# characters (after resolving a doc() block) and assert the full text round-trips.
# Teradata stores comments in DBC ...CommentString (VARCHAR(255)), so text beyond
# 255 chars cannot round-trip. The Teradata relation/column round-trip is instead
# covered by TestPersistDocsRoundtripTeradata below using <=255 char descriptions.
# The remaining shared classes use short fixtures that fit the limit.
# ---------------------------------------------------------------------------
class TestPersistDocsColumnMissingTeradata(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsAllColumnsMissingTeradata(BasePersistDocsAllColumnsMissing):
    pass


class TestPersistDocsQuotedColumnCaseSensitiveTeradata(
    BasePersistDocsQuotedColumnCaseSensitive
):
    pass


class TestPersistDocsQuotedDescriptionNotAppliedOnMismatchTeradata(
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch
):
    pass


class TestPersistDocsCommentOnQuotedColumnTeradata(BasePersistDocsCommentOnQuotedColumn):
    pass


# ---------------------------------------------------------------------------
# Teradata-specific coverage
# ---------------------------------------------------------------------------
_MODEL_SQL = "select 1 as id, 'a' as name"

_LONG_DESC = "x" * 400  # exceeds the 255-char Teradata comment limit

_SCHEMA_YML = """
version: 2
models:
  - name: persist_model
    description: "Relation level description for persist_docs test"
    columns:
      - name: id
        description: "The id column description"
      - name: name
        description: "The name column description"
"""

_SCHEMA_LONG_YML = """
version: 2
models:
  - name: long_desc_model
    description: "{long}"
""".replace("{long}", _LONG_DESC)


class TestPersistDocsRoundtripTeradata:
    """Teradata-native equivalent of the flagship BasePersistDocs test: relation and
    column descriptions (<=255 chars) round-trip into catalog.json via COMMENT ON."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"persist_model.sql": _MODEL_SQL, "schema.yml": _SCHEMA_YML}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "materialized": "table",
                    "+persist_docs": {"relation": True, "columns": True},
                }
            }
        }

    def test_comments_roundtrip_to_catalog(self, project):
        run_dbt(["run"])
        run_dbt(["docs", "generate"])
        catalog_path = os.path.join(project.project_root, "target", "catalog.json")
        with open(catalog_path) as fp:
            catalog = json.load(fp)
        node = catalog["nodes"]["model.test.persist_model"]
        assert node["metadata"]["comment"].startswith("Relation level description")
        assert node["columns"]["id"]["comment"].startswith("The id column description")
        assert node["columns"]["name"]["comment"].startswith("The name column description")


_INCR_IDEMPOTENT = (
    "{{ config(materialized='incremental') }}\n"
    "select 1 as id, cast('a' as varchar(10)) as name"
)


class TestPersistDocsIdempotentTeradata:
    """A second run with unchanged descriptions must issue no COMMENT ON DDL.

    Uses an *incremental* model: table/view materializations drop-and-recreate the
    object every run (so the comment is legitimately re-applied), whereas an
    incremental relation persists across runs, which is where change detection
    (skip DDL when the comment is unchanged) actually applies.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"persist_model.sql": _INCR_IDEMPOTENT, "schema.yml": _SCHEMA_YML}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "materialized": "incremental",
                    "+persist_docs": {"relation": True, "columns": True},
                }
            }
        }

    def test_no_ddl_on_unchanged_rerun(self, project):
        run_dbt(["run"])
        # Second run: object persists and descriptions are unchanged -> no COMMENT DDL.
        # COMMENT DDL is only emitted at DEBUG level, so capture with --debug.
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert "comment on table" not in logs.lower()
        assert "comment on column" not in logs.lower()


class TestPersistDocsLongCommentTeradata:
    """A description longer than 255 chars is truncated and the run succeeds."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"long_desc_model.sql": _MODEL_SQL, "schema.yml": _SCHEMA_LONG_YML}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "materialized": "table",
                    "+persist_docs": {"relation": True, "columns": False},
                }
            }
        }

    def test_long_comment_truncated_ok(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


# ---------------------------------------------------------------------------
# Per-materialization coverage (view / incremental / seed) in a single build
# to minimize round-trips against the (latency-heavy) database.
# ---------------------------------------------------------------------------
_VIEW_MODEL = "{{ config(materialized='view') }}\nselect 1 as id, cast('a' as varchar(10)) as nm"
_INCR_MODEL = "{{ config(materialized='incremental') }}\nselect 1 as id, cast('a' as varchar(10)) as nm"
_SEED_CSV = "id,nm\n1,a\n2,b\n"

_MATS_SCHEMA_YML = """
version: 2
models:
  - name: view_mat
    description: "View materialization relation description"
    columns:
      - name: id
        description: "View id column description"
  - name: incr_mat
    description: "Incremental materialization relation description"
    columns:
      - name: id
        description: "Incremental id column description"
seeds:
  - name: seed_mat
    description: "Seed relation description"
    columns:
      - name: nm
        description: "Seed nm column description"
"""


class TestPersistDocsMaterializationsTeradata:
    """persist_docs works across view, incremental and seed materializations."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_mat.csv": _SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_mat.sql": _VIEW_MODEL,
            "incr_mat.sql": _INCR_MODEL,
            "schema.yml": _MATS_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        pd = {"relation": True, "columns": True}
        return {
            "models": {"test": {"+persist_docs": pd}},
            "seeds": {"test": {"+persist_docs": pd}},
        }

    def test_comments_on_all_materializations(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(["docs", "generate"])
        with open(os.path.join(project.project_root, "target", "catalog.json")) as fp:
            catalog = json.load(fp)
        nodes = catalog["nodes"]

        view_node = nodes["model.test.view_mat"]
        assert view_node["metadata"]["comment"].startswith("View materialization relation")
        assert view_node["columns"]["id"]["comment"].startswith("View id column")

        incr_node = nodes["model.test.incr_mat"]
        assert incr_node["metadata"]["comment"].startswith("Incremental materialization relation")
        assert incr_node["columns"]["id"]["comment"].startswith("Incremental id column")

        seed_node = nodes["seed.test.seed_mat"]
        assert seed_node["metadata"]["comment"].startswith("Seed relation description")
        assert seed_node["columns"]["nm"]["comment"].startswith("Seed nm column")


# ---------------------------------------------------------------------------
# Special characters must be escaped and round-trip intact.
# ---------------------------------------------------------------------------
_SPECIAL_DESC = "It's a \"quoted\" desc; with -- dash and /* block */ tokens"
# Use YAML block scalars so embedded single/double quotes need no YAML escaping.
# `|-` strips the trailing newline, so the value equals _SPECIAL_DESC exactly.
_SPECIAL_SCHEMA_YML = """
version: 2
models:
  - name: special_model
    description: |-
      It's a "quoted" desc; with -- dash and /* block */ tokens
    columns:
      - name: id
        description: |-
          col with O'Brien apostrophe
"""


class TestPersistDocsSpecialCharsTeradata:
    """Single quotes, double quotes and SQL-comment tokens are escaped and round-trip."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"special_model.sql": _MODEL_SQL, "schema.yml": _SPECIAL_SCHEMA_YML}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "materialized": "table",
                    "+persist_docs": {"relation": True, "columns": True},
                }
            }
        }

    def test_special_chars_roundtrip(self, project):
        run_dbt(["run"])
        run_dbt(["docs", "generate"])
        with open(os.path.join(project.project_root, "target", "catalog.json")) as fp:
            catalog = json.load(fp)
        node = catalog["nodes"]["model.test.special_model"]
        assert node["metadata"]["comment"] == _SPECIAL_DESC
        assert node["columns"]["id"]["comment"] == "col with O'Brien apostrophe"


# ---------------------------------------------------------------------------
# A changed description re-issues exactly the comment DDL (complement to idempotency).
# ---------------------------------------------------------------------------
_SCHEMA_V1 = """
version: 2
models:
  - name: change_model
    description: "Original description"
"""
_SCHEMA_V2 = """
version: 2
models:
  - name: change_model
    description: "Updated description"
"""


class TestPersistDocsChangedDescriptionTeradata:
    """Changing the description re-issues the relation COMMENT and the new text persists."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"change_model.sql": _MODEL_SQL, "schema.yml": _SCHEMA_V1}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "materialized": "table",
                    "+persist_docs": {"relation": True, "columns": False},
                }
            }
        }

    def test_changed_description_reissues_ddl(self, project):
        run_dbt(["run"])
        # rewrite the schema with a new description, then re-run
        schema_path = os.path.join(project.project_root, "models", "schema.yml")
        with open(schema_path, "w") as fp:
            fp.write(_SCHEMA_V2)
        # COMMENT DDL is only emitted at DEBUG level.
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert "comment on table" in logs.lower()
        run_dbt(["docs", "generate"])
        with open(os.path.join(project.project_root, "target", "catalog.json")) as fp:
            catalog = json.load(fp)
        node = catalog["nodes"]["model.test.change_model"]
        assert node["metadata"]["comment"].startswith("Updated description")


# ---------------------------------------------------------------------------
# Snapshot materialization persists docs.
# ---------------------------------------------------------------------------
_SNAPSHOT_SQL = """
{% snapshot cmt_snapshot %}
{{ config(target_schema=schema, unique_key='id', strategy='check', check_cols=['nm']) }}
select 1 as id, cast('a' as varchar(10)) as nm
{% endsnapshot %}
"""
_SNAPSHOT_SCHEMA_YML = """
version: 2
snapshots:
  - name: cmt_snapshot
    description: "Snapshot relation description"
    columns:
      - name: id
        description: "Snapshot id column description"
"""


class TestPersistDocsSnapshotTeradata:
    """persist_docs works for the snapshot materialization."""

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"cmt_snapshot.sql": _SNAPSHOT_SQL}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": _SNAPSHOT_SCHEMA_YML}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "snapshots": {"test": {"+persist_docs": {"relation": True, "columns": True}}}
        }

    def test_snapshot_comment_persists(self, project):
        run_dbt(["snapshot"])
        run_dbt(["docs", "generate"])
        with open(os.path.join(project.project_root, "target", "catalog.json")) as fp:
            catalog = json.load(fp)
        node = catalog["nodes"]["snapshot.test.cmt_snapshot"]
        assert node["metadata"]["comment"].startswith("Snapshot relation description")
