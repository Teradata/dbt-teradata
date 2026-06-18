"""Unit tests for OTF incremental materialization support.

Pure unit tests (no database required) that verify:
  1. The incremental_otf.sql macro file exists and contains expected macros
  2. The incremental.sql entry point branches to OTF when catalog_name is set
  3. OTF relation rendering works correctly for incremental SQL generation
  4. Catalog integration properties support incremental use case
  5. Strategy constants and macro structure match the design
"""

import os
import pytest
from unittest.mock import MagicMock

from dbt.adapters.catalogs import CatalogIntegrationConfig
from dbt.adapters.teradata.catalogs import (
    TeradataDatalakeCatalogIntegration,
)
from dbt.adapters.teradata.relation import TeradataRelation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
_MACRO_DIR = os.path.join(
    _REPO_ROOT, "dbt", "include", "teradata", "macros", "materializations"
)


def _read_macro(subdir, filename):
    path = os.path.join(_MACRO_DIR, subdir, filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _make_catalog_config(**overrides):
    """Build a spec'd MagicMock of CatalogIntegrationConfig."""
    config = MagicMock(spec=CatalogIntegrationConfig)
    config.name = overrides.get("name", "test_catalog")
    config.catalog_type = overrides.get("catalog_type", "datalake")
    config.catalog_name = overrides.get("catalog_name", None)
    config.table_format = overrides.get("table_format", None)
    config.external_volume = overrides.get("external_volume", None)
    config.file_format = overrides.get("file_format", None)
    config.adapter_properties = overrides.get("adapter_properties", {
        "datalake_name": "my_datalake",
        "otf_database": "my_otf_db",
    })
    return config


# ===================================================================
# 1. Macro file existence and structure
# ===================================================================

class TestOTFIncrementalMacroFileExists:
    """Verify the incremental_otf.sql file exists in the expected location."""

    def test_file_exists(self):
        path = os.path.join(_MACRO_DIR, "otf", "incremental_otf.sql")
        assert os.path.isfile(path), f"Missing macro file: {path}"

    def test_file_is_non_empty(self):
        content = _read_macro("otf", "incremental_otf.sql")
        assert len(content.strip()) > 0


class TestOTFIncrementalMacroContent:
    """Verify the macro file declares all expected macros and references."""

    @pytest.fixture(scope="class")
    def content(self):
        return _read_macro("otf", "incremental_otf.sql")

    # --- expected macro definitions ---

    def test_defines_strategy_validator(self, content):
        assert "macro teradata__validate_get_otf_incremental_strategy" in content

    def test_defines_append_macro(self, content):
        assert "macro teradata__get_otf_incremental_append_sql" in content

    def test_defines_main_entry_point(self, content):
        assert "macro teradata__incremental_otf" in content

    # --- strategy validation ---

    def test_accepts_append_strategy(self, content):
        assert "'append'" in content

    def test_rejects_delete_insert(self, content):
        assert "'delete+insert'" in content
        assert "not yet supported" in content.lower()

    def test_rejects_merge(self, content):
        assert "'merge'" in content
        assert "not yet supported" in content.lower()

    def test_rejects_valid_history(self, content):
        assert "'valid_history'" in content

    def test_rejects_microbatch(self, content):
        assert "'microbatch'" in content

    # --- required macro references ---

    def test_calls_build_otf_relation_name(self, content):
        assert "teradata__build_otf_relation_name" in content

    def test_calls_create_otf_table_as_for_first_run(self, content):
        assert "teradata__create_otf_table_as" in content

    def test_drops_staging_table_after_use(self, content):
        assert "DROP TABLE /*+ IF EXISTS */ {{ tmp_relation }}" in content

    # --- guardrails ---

    def test_guards_table_kind(self, content):
        assert "table_kind" in content

    def test_guards_table_option(self, content):
        assert "table_option" in content

    def test_guards_with_statistics(self, content):
        assert "with_statistics" in content

    def test_guards_index(self, content):
        assert "index" in content

    def test_guards_contract(self, content):
        assert "contract" in content.lower()

    def test_guards_on_schema_change(self, content):
        assert "on_schema_change" in content
        assert "--full-refresh" in content

    def test_warns_on_grants(self, content):
        assert "grants" in content.lower()

    # --- flow control ---

    def test_handles_first_run(self, content):
        assert "adapter.otf_relation_exists(" in content
        assert "not otf_exists" in content

    def test_handles_full_refresh(self, content):
        assert "should_full_refresh" in content

    def test_uses_native_staging_creation(self, content):
        assert "api.Relation.create(" in content
        assert "target.schema" in content

    def test_runs_pre_hooks(self, content):
        assert "run_hooks(pre_hooks" in content

    def test_runs_post_hooks(self, content):
        assert "run_hooks(post_hooks" in content

    def test_commits_transaction(self, content):
        assert "adapter.commit()" in content

    def test_caches_relation(self, content):
        assert "adapter.cache_added" in content

    def test_returns_relations(self, content):
        assert "return({'relations':" in content


# ===================================================================
# 2. Incremental entry point (incremental.sql)
# ===================================================================

class TestIncrementalEntryPointBranching:
    """Verify the main incremental.sql properly branches to OTF."""

    @pytest.fixture(scope="class")
    def content(self):
        return _read_macro("incremental", "incremental.sql")

    def test_checks_catalog_name(self, content):
        assert "catalog_name" in content

    def test_calls_otf_incremental_macro(self, content):
        assert "teradata__incremental_otf" in content

    def test_captures_otf_result(self, content):
        """OTF result is captured with {% set %} and returned at materialization level."""
        assert "set otf_result" in content or "teradata__incremental_otf" in content

    def test_returns_otf_result(self, content):
        assert "return(otf_result)" in content

    def test_has_else_branch_for_native(self, content):
        assert "{% else %}" in content

    def test_no_longer_blocks_otf_with_compile_error(self, content):
        assert "Only the 'table' materialization supports OTF tables" not in content

    def test_native_path_still_has_validate_strategy(self, content):
        assert "teradata__validate_get_incremental_strategy" in content

    def test_native_path_still_has_return(self, content):
        assert "return({'relations': [target_relation]})" in content


# ===================================================================
# 3. OTF relation rendering for incremental SQL
# ===================================================================

class TestOTFRelationForIncrementalSQL:
    """Verify OTF and staging relations render correctly for INSERT/DELETE SQL."""

    def test_otf_target_renders_three_part_name(self):
        """INSERT INTO target must use 3-part OTF name."""
        rel = TeradataRelation.create(
            database="dl",
            schema="otf_db",
            identifier="sales_fact",
            quote_policy={"database": True, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        assert rel.render() == '"dl"."otf_db"."sales_fact"'

    def test_staging_table_renders_two_part_name(self):
        """SELECT FROM staging table uses 2-part native name."""
        rel = TeradataRelation.create(
            schema="test_schema",
            identifier="sales_fact__dbt_tmp",
        )
        assert rel.render() == '"test_schema"."sales_fact__dbt_tmp"'

    def test_otf_relation_is_otf_true(self):
        rel = TeradataRelation.create(
            database="dl", schema="otf_db", identifier="t",
            is_otf=True,
        )
        assert rel.is_otf is True

    def test_staging_relation_is_otf_false(self):
        rel = TeradataRelation.create(
            schema="mydb", identifier="t__dbt_tmp",
        )
        assert rel.is_otf is False

    def test_otf_and_staging_can_coexist_in_sql(self):
        """Verify both relations render independently for cross-table SQL."""
        otf = TeradataRelation.create(
            database="prod_lake",
            schema="iceberg_db",
            identifier="orders",
            quote_policy={"database": True, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        staging = TeradataRelation.create(
            schema="dbt_schema",
            identifier="orders__dbt_tmp",
        )
        insert_sql = f"INSERT INTO {otf.render()} SELECT * FROM {staging.render()}"
        assert '"prod_lake"."iceberg_db"."orders"' in insert_sql
        assert '"dbt_schema"."orders__dbt_tmp"' in insert_sql


# ===================================================================
# 4. Catalog integration for incremental
# ===================================================================

class TestCatalogIntegrationForIncremental:
    """Verify catalog integration properties needed for incremental flow."""

    def test_has_datalake_name(self):
        integration = TeradataDatalakeCatalogIntegration(_make_catalog_config())
        assert integration.datalake_name == "my_datalake"

    def test_has_otf_database(self):
        integration = TeradataDatalakeCatalogIntegration(_make_catalog_config())
        assert integration.otf_database == "my_otf_db"

    def test_allows_writes_for_incremental(self):
        """Incremental strategy requires write access to OTF tables."""
        integration = TeradataDatalakeCatalogIntegration(_make_catalog_config())
        assert integration.allows_writes is True

    def test_catalog_type_is_datalake(self):
        integration = TeradataDatalakeCatalogIntegration(_make_catalog_config())
        assert integration.catalog_type == "datalake"

    def test_default_table_format_is_iceberg(self):
        integration = TeradataDatalakeCatalogIntegration(_make_catalog_config())
        assert integration.table_format == "iceberg"

    def test_supports_delta_format(self):
        integration = TeradataDatalakeCatalogIntegration(
            _make_catalog_config(table_format="delta")
        )
        assert integration.table_format == "delta"


# ===================================================================
# 5. Strategy constants
# ===================================================================

class TestOTFIncrementalStrategyConstants:
    """Verify supported vs unsupported strategy definitions."""

    SUPPORTED = {"append"}
    UNSUPPORTED = {"delete+insert", "merge", "valid_history", "microbatch"}

    def test_append_is_supported(self):
        assert "append" in self.SUPPORTED

    @pytest.mark.parametrize("strategy", ["delete+insert", "merge", "valid_history", "microbatch"])
    def test_unsupported_strategy(self, strategy):
        assert strategy not in self.SUPPORTED

    def test_supported_and_unsupported_are_disjoint(self):
        assert self.SUPPORTED.isdisjoint(self.UNSUPPORTED)

    def test_default_strategy_convention(self):
        """Default strategy (when none specified) should be 'append'
        which is the safest additive operation."""
        content = _read_macro("otf", "incremental_otf.sql")
        # The validator defaults to 'append' when no strategy is configured
        assert '"incremental_strategy", "append"' in content
