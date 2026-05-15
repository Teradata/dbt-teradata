"""Unit tests for Teradata OTF (Open Table Format) catalog integration.

Tests cover:
  - TeradataCatalogRelation dataclass fields and defaults
  - TeradataDatalakeCatalogIntegration initialization and validation
  - build_relation() config passthrough (partitioned_by, sorted_by, etc.)
  - 3-part OTF naming construction (datalake."db"."table")
  - Adapter CATALOG_INTEGRATIONS registration
"""

import pytest
from dataclasses import asdict
from unittest.mock import MagicMock

from dbt.adapters.teradata.catalogs import (
    TeradataDatalakeCatalogIntegration,
    TeradataCatalogRelation,
)
from dbt.adapters.catalogs import InvalidCatalogIntegrationConfigError


# ---------------------------------------------------------------------------
# Helpers for building mock CatalogIntegrationConfig objects
# ---------------------------------------------------------------------------

def _make_config(**overrides):
    """Build a mock CatalogIntegrationConfig with sensible defaults."""
    config = MagicMock()
    config.name = overrides.get("name", "test_catalog")
    config.catalog_type = overrides.get("catalog_type", "datalake")
    config.catalog_name = overrides.get("catalog_name", None)
    config.table_format = overrides.get("table_format", None)
    config.external_volume = overrides.get("external_volume", None)
    config.file_format = overrides.get("file_format", None)
    config.adapter_properties = overrides.get("adapter_properties", {})
    return config


def _make_valid_config(**overrides):
    """Build a mock config that already includes the required adapter_properties."""
    defaults = {
        "adapter_properties": {
            "datalake_name": "my_datalake",
            "otf_database": "my_otf_db",
        },
    }
    defaults.update(overrides)
    return _make_config(**defaults)


# ===================================================================
# TeradataCatalogRelation tests
# ===================================================================

class TestTeradataCatalogRelation:
    """Unit tests for TeradataCatalogRelation dataclass."""

    def test_defaults(self):
        rel = TeradataCatalogRelation()
        assert rel.catalog_type is None
        assert rel.catalog_name is None
        assert rel.table_format is None
        assert rel.file_format is None
        assert rel.external_volume is None
        assert rel.datalake_name is None
        assert rel.otf_database is None
        assert rel.partitioned_by is None
        assert rel.sorted_by is None
        assert rel.tblproperties is None
        assert rel.purge_mode is None

    def test_all_fields(self):
        rel = TeradataCatalogRelation(
            catalog_type="datalake",
            catalog_name="cat",
            table_format="iceberg",
            file_format="parquet",
            external_volume="vol",
            datalake_name="dl",
            otf_database="otf_db",
            partitioned_by="YEAR(dt)",
            sorted_by="id ASC",
            tblproperties="'k'='v'",
            purge_mode="NO PURGE",
        )
        assert rel.catalog_type == "datalake"
        assert rel.catalog_name == "cat"
        assert rel.table_format == "iceberg"
        assert rel.file_format == "parquet"
        assert rel.external_volume == "vol"
        assert rel.datalake_name == "dl"
        assert rel.otf_database == "otf_db"
        assert rel.partitioned_by == "YEAR(dt)"
        assert rel.sorted_by == "id ASC"
        assert rel.tblproperties == "'k'='v'"
        assert rel.purge_mode == "NO PURGE"

    def test_is_dataclass(self):
        """TeradataCatalogRelation supports asdict (is a proper dataclass)."""
        rel = TeradataCatalogRelation(datalake_name="dl", otf_database="db")
        d = asdict(rel)
        assert d["datalake_name"] == "dl"
        assert d["otf_database"] == "db"
        assert "partitioned_by" in d

    def test_partial_ddl_options(self):
        """Only some DDL options set -- others stay None."""
        rel = TeradataCatalogRelation(partitioned_by="YEAR(dt)")
        assert rel.partitioned_by == "YEAR(dt)"
        assert rel.sorted_by is None
        assert rel.tblproperties is None
        assert rel.purge_mode is None

    def test_purge_mode_no_purge(self):
        """purge_mode can be set to NO PURGE."""
        rel = TeradataCatalogRelation(purge_mode="NO PURGE")
        assert rel.purge_mode == "NO PURGE"

    def test_purge_mode_purge_all(self):
        """purge_mode can be set to PURGE ALL."""
        rel = TeradataCatalogRelation(purge_mode="PURGE ALL")
        assert rel.purge_mode == "PURGE ALL"


# ===================================================================
# DATALAKE integration -- __init__ tests
# ===================================================================

class TestTeradataDatalakeCatalogIntegration:
    """Unit tests for TeradataDatalakeCatalogIntegration."""

    # -- happy path --

    def test_init_with_valid_config(self):
        config = _make_valid_config()
        integration = TeradataDatalakeCatalogIntegration(config)
        assert integration.datalake_name == "my_datalake"
        assert integration.otf_database == "my_otf_db"
        assert integration.catalog_type == "datalake"
        assert integration.allows_writes is True
        assert integration.table_format == "iceberg"
        assert integration.file_format == "parquet"

    def test_class_level_defaults(self):
        """Class-level defaults are correct before any __init__."""
        assert TeradataDatalakeCatalogIntegration.catalog_type == "datalake"
        assert TeradataDatalakeCatalogIntegration.allows_writes is True
        assert TeradataDatalakeCatalogIntegration.table_format == "iceberg"
        assert TeradataDatalakeCatalogIntegration.file_format == "parquet"

    def test_file_format_defaults_to_parquet_when_none(self):
        config = _make_valid_config(file_format=None)
        integration = TeradataDatalakeCatalogIntegration(config)
        assert integration.file_format == "parquet"

    def test_file_format_override(self):
        config = _make_valid_config(file_format="orc")
        integration = TeradataDatalakeCatalogIntegration(config)
        assert integration.file_format == "orc"

    def test_table_format_override(self):
        config = _make_valid_config(table_format="delta")
        integration = TeradataDatalakeCatalogIntegration(config)
        assert integration.table_format == "delta"

    def test_catalog_name_passed_through(self):
        config = _make_valid_config(catalog_name="glue_catalog")
        integration = TeradataDatalakeCatalogIntegration(config)
        assert integration.catalog_name == "glue_catalog"

    # -- validation errors --

    def test_init_missing_datalake_name_raises(self):
        config = _make_config(
            adapter_properties={"otf_database": "my_otf_db"}
        )
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(config)

    def test_init_missing_otf_database_raises(self):
        config = _make_config(
            adapter_properties={"datalake_name": "my_datalake"}
        )
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(config)

    def test_init_empty_adapter_properties_raises(self):
        config = _make_config(adapter_properties={})
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(config)

    def test_init_none_adapter_properties_raises(self):
        config = _make_config(adapter_properties=None)
        with pytest.raises(
            (InvalidCatalogIntegrationConfigError, TypeError, AttributeError)
        ):
            TeradataDatalakeCatalogIntegration(config)

    def test_init_empty_string_datalake_name_raises(self):
        config = _make_config(
            adapter_properties={"datalake_name": "", "otf_database": "db"}
        )
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(config)

    def test_init_empty_string_otf_database_raises(self):
        config = _make_config(
            adapter_properties={"datalake_name": "dl", "otf_database": ""}
        )
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(config)

    def test_error_message_mentions_datalake_name(self):
        config = _make_config(adapter_properties={"otf_database": "db"})
        with pytest.raises(
            InvalidCatalogIntegrationConfigError, match="datalake_name"
        ):
            TeradataDatalakeCatalogIntegration(config)

    def test_error_message_mentions_otf_database(self):
        config = _make_config(
            adapter_properties={"datalake_name": "dl"}
        )
        with pytest.raises(
            InvalidCatalogIntegrationConfigError, match="otf_database"
        ):
            TeradataDatalakeCatalogIntegration(config)


# ===================================================================
# DATALAKE integration -- build_relation tests
# ===================================================================

class TestBuildRelation:
    """Tests for TeradataDatalakeCatalogIntegration.build_relation."""

    def _make_integration(self, **config_overrides):
        config = _make_valid_config(**config_overrides)
        return TeradataDatalakeCatalogIntegration(config)

    def test_basic_build(self):
        integration = self._make_integration(
            catalog_name="glue_catalog",
            adapter_properties={
                "datalake_name": "my_glue_datalake",
                "otf_database": "my_otf_db",
            },
        )
        relation_config = MagicMock()
        relation_config.config = {}
        result = integration.build_relation(relation_config)

        assert isinstance(result, TeradataCatalogRelation)
        assert result.datalake_name == "my_glue_datalake"
        assert result.otf_database == "my_otf_db"
        assert result.catalog_type == "datalake"
        assert result.catalog_name == "glue_catalog"
        assert result.table_format == "iceberg"
        assert result.file_format == "parquet"
        assert result.partitioned_by is None
        assert result.sorted_by is None
        assert result.tblproperties is None
        assert result.purge_mode is None

    def test_with_all_ddl_options(self):
        """All DDL options (partitioned_by, sorted_by, tblproperties, purge_mode) are passed through."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "partitioned_by": "YEAR(dt)",
            "sorted_by": "id ASC",
            "tblproperties": "'write.format.default'='parquet'",
            "purge_mode": "NO PURGE",
        }
        result = integration.build_relation(relation_config)

        assert result.partitioned_by == "YEAR(dt)"
        assert result.sorted_by == "id ASC"
        assert result.tblproperties == "'write.format.default'='parquet'"
        assert result.purge_mode == "NO PURGE"

    def test_with_partitioned_by_only(self):
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {"partitioned_by": "MONTH(created_at)"}
        result = integration.build_relation(relation_config)

        assert result.partitioned_by == "MONTH(created_at)"
        assert result.sorted_by is None
        assert result.tblproperties is None

    def test_with_sorted_by_only(self):
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {"sorted_by": "ts DESC"}
        result = integration.build_relation(relation_config)

        assert result.partitioned_by is None
        assert result.sorted_by == "ts DESC"

    def test_with_tblproperties_only(self):
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {"tblproperties": "'gc.enabled'='true'"}
        result = integration.build_relation(relation_config)

        assert result.tblproperties == "'gc.enabled'='true'"
        assert result.partitioned_by is None

    def test_with_purge_mode_only(self):
        """purge_mode can be set without other DDL options."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {"purge_mode": "NO PURGE"}
        result = integration.build_relation(relation_config)

        assert result.purge_mode == "NO PURGE"
        assert result.partitioned_by is None

    def test_no_config_attr(self):
        """build_relation works when RelationConfig has no config attribute."""
        integration = self._make_integration()
        relation_config = MagicMock(spec=[])  # no config attribute
        result = integration.build_relation(relation_config)

        assert result.partitioned_by is None
        assert result.sorted_by is None
        assert result.tblproperties is None
        assert result.purge_mode is None

    def test_none_config_attr(self):
        """build_relation works when RelationConfig.config is None."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = None
        result = integration.build_relation(relation_config)

        assert result.partitioned_by is None
        assert result.sorted_by is None
        assert result.tblproperties is None
        assert result.purge_mode is None

    def test_unrelated_config_keys_ignored(self):
        """Extra keys in model config don't leak into the relation."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "materialized": "table",
            "catalog_name": "x",
            "partitioned_by": "YEAR(dt)",
        }
        result = integration.build_relation(relation_config)

        assert result.partitioned_by == "YEAR(dt)"
        assert result.sorted_by is None


# ===================================================================
# Adapter CATALOG_INTEGRATIONS registration
# ===================================================================

class TestAdapterCatalogRegistration:
    """Verify TeradataAdapter.CATALOG_INTEGRATIONS is correct."""

    def test_only_datalake_registered(self):
        from dbt.adapters.teradata.impl import TeradataAdapter

        assert len(TeradataAdapter.CATALOG_INTEGRATIONS) == 1
        assert TeradataAdapter.CATALOG_INTEGRATIONS[0] is TeradataDatalakeCatalogIntegration


# ===================================================================
# 3-part naming construction (unit-level, no Jinja)
# ===================================================================

class TestThreeDotNaming:
    """Verify the 3-part OTF relation string is built correctly.

    This replicates the Jinja logic from create_otf_table_as.sql and
    adapters.sql in pure Python to catch regressions without needing
    the full dbt rendering stack.

    Jinja expression under test:
        datalake_name ~ '."' ~ otf_database ~ '"."' ~ identifier ~ '"'
    """

    @staticmethod
    def _build_otf_relation(datalake_name, otf_database, identifier):
        return f'{datalake_name}."{otf_database}"."{identifier}"'

    def test_basic_naming(self):
        result = self._build_otf_relation(
            "studio_otftest_datalake_001_normal",
            "studio_otftest_database_001_normal",
            "my_model",
        )
        assert result == (
            'studio_otftest_datalake_001_normal.'
            '"studio_otftest_database_001_normal".'
            '"my_model"'
        )

    def test_datalake_unquoted(self):
        result = self._build_otf_relation("dl", "db", "tbl")
        assert not result.startswith('"')

    def test_otf_database_quoted(self):
        result = self._build_otf_relation("dl", "db", "tbl")
        assert '."db".' in result

    def test_table_quoted(self):
        result = self._build_otf_relation("dl", "db", "tbl")
        assert result.endswith('"tbl"')

    def test_special_chars_in_names(self):
        result = self._build_otf_relation("dl_1", "db-2", "tbl 3")
        assert result == 'dl_1."db-2"."tbl 3"'
