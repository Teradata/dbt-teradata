"""Unit tests for Teradata OTF (Open Table Format) catalog integration.

Covers:
  - TeradataCatalogRelation dataclass fields and defaults
  - TeradataDatalakeCatalogIntegration initialization and validation
  - build_relation() config passthrough
  - Adapter CATALOG_INTEGRATIONS registration
  - TeradataRelation.render() for both 2-part (native) and 3-part (OTF) names
  - is_otf invariant guard in render()

These tests are pure unit tests (no database required) and live in tests/unit/
rather than tests/functional/ so they can run in a fast CI matrix without
provisioning Vantage Express.
"""

import pytest
from dataclasses import asdict
from unittest.mock import MagicMock

from dbt.adapters.catalogs import (
    CatalogIntegrationConfig,
    InvalidCatalogIntegrationConfigError,
)
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.teradata.catalogs import (
    TeradataDatalakeCatalogIntegration,
    TeradataCatalogRelation,
)
from dbt.adapters.teradata.relation import TeradataRelation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**overrides):
    """Build a spec'd MagicMock of CatalogIntegrationConfig.

    Using spec= ensures the mock fails on attribute access for anything
    not on the protocol — masking-by-MagicMock would defeat the test.
    """
    config = MagicMock(spec=CatalogIntegrationConfig)
    config.name = overrides.get("name", "test_catalog")
    config.catalog_type = overrides.get("catalog_type", "datalake")
    config.catalog_name = overrides.get("catalog_name", None)
    config.table_format = overrides.get("table_format", None)
    config.external_volume = overrides.get("external_volume", None)
    config.file_format = overrides.get("file_format", None)
    config.adapter_properties = overrides.get("adapter_properties", {})
    return config


def _make_valid_config(**overrides):
    defaults = {
        "adapter_properties": {
            "datalake_name": "my_datalake",
            "otf_database": "my_otf_db",
        },
    }
    defaults.update(overrides)
    return _make_config(**defaults)


# ===================================================================
# TeradataCatalogRelation
# ===================================================================

class TestTeradataCatalogRelation:
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
        assert rel.datalake_name == "dl"
        assert rel.otf_database == "otf_db"
        assert rel.partitioned_by == "YEAR(dt)"
        assert rel.sorted_by == "id ASC"
        assert rel.tblproperties == "'k'='v'"
        assert rel.purge_mode == "NO PURGE"

    def test_is_dataclass(self):
        rel = TeradataCatalogRelation(datalake_name="dl", otf_database="db")
        d = asdict(rel)
        assert d["datalake_name"] == "dl"
        assert d["otf_database"] == "db"
        assert "partitioned_by" in d

    def test_purge_mode_no_purge(self):
        rel = TeradataCatalogRelation(purge_mode="NO PURGE")
        assert rel.purge_mode == "NO PURGE"

    def test_purge_mode_purge_all(self):
        rel = TeradataCatalogRelation(purge_mode="PURGE ALL")
        assert rel.purge_mode == "PURGE ALL"


# ===================================================================
# TeradataDatalakeCatalogIntegration -- __init__
# ===================================================================

class TestTeradataDatalakeCatalogIntegration:
    # -- happy path --

    def test_init_with_valid_config(self):
        integration = TeradataDatalakeCatalogIntegration(_make_valid_config())
        assert integration.datalake_name == "my_datalake"
        assert integration.otf_database == "my_otf_db"
        assert integration.catalog_type == "datalake"
        assert integration.allows_writes is True
        assert integration.table_format == "iceberg"
        assert integration.file_format == "parquet"

    def test_class_level_defaults(self):
        assert TeradataDatalakeCatalogIntegration.catalog_type == "datalake"
        assert TeradataDatalakeCatalogIntegration.allows_writes is True
        assert TeradataDatalakeCatalogIntegration.table_format == "iceberg"
        assert TeradataDatalakeCatalogIntegration.file_format == "parquet"

    def test_file_format_defaults_to_parquet_when_none(self):
        integration = TeradataDatalakeCatalogIntegration(_make_valid_config(file_format=None))
        assert integration.file_format == "parquet"

    def test_file_format_override(self):
        integration = TeradataDatalakeCatalogIntegration(_make_valid_config(file_format="orc"))
        assert integration.file_format == "orc"

    def test_table_format_override(self):
        integration = TeradataDatalakeCatalogIntegration(_make_valid_config(table_format="delta"))
        assert integration.table_format == "delta"

    def test_catalog_name_passed_through(self):
        integration = TeradataDatalakeCatalogIntegration(_make_valid_config(catalog_name="glue_catalog"))
        assert integration.catalog_name == "glue_catalog"

    # -- validation errors --

    def test_init_missing_datalake_name_raises(self):
        config = _make_config(adapter_properties={"otf_database": "my_otf_db"})
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(config)

    def test_init_missing_otf_database_raises(self):
        config = _make_config(adapter_properties={"datalake_name": "my_datalake"})
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(config)

    def test_init_empty_adapter_properties_raises(self):
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(_make_config(adapter_properties={}))

    def test_init_none_adapter_properties_raises(self):
        # Pinned to InvalidCatalogIntegrationConfigError only: the production
        # code coerces None via `or {}`, so this is the only path that fires.
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(_make_config(adapter_properties=None))

    def test_init_empty_string_datalake_name_raises(self):
        config = _make_config(adapter_properties={"datalake_name": "", "otf_database": "db"})
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(config)

    def test_init_empty_string_otf_database_raises(self):
        config = _make_config(adapter_properties={"datalake_name": "dl", "otf_database": ""})
        with pytest.raises(InvalidCatalogIntegrationConfigError):
            TeradataDatalakeCatalogIntegration(config)

    def test_error_message_mentions_datalake_name(self):
        with pytest.raises(InvalidCatalogIntegrationConfigError, match="datalake_name"):
            TeradataDatalakeCatalogIntegration(_make_config(adapter_properties={"otf_database": "db"}))

    def test_error_message_mentions_otf_database(self):
        with pytest.raises(InvalidCatalogIntegrationConfigError, match="otf_database"):
            TeradataDatalakeCatalogIntegration(_make_config(adapter_properties={"datalake_name": "dl"}))


# ===================================================================
# build_relation()
# ===================================================================

class TestBuildRelation:
    def _make_integration(self, **config_overrides):
        return TeradataDatalakeCatalogIntegration(_make_valid_config(**config_overrides))

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
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {"purge_mode": "NO PURGE"}
        result = integration.build_relation(relation_config)
        assert result.purge_mode == "NO PURGE"
        assert result.partitioned_by is None

    def test_no_config_attr(self):
        integration = self._make_integration()
        relation_config = MagicMock(spec=[])
        result = integration.build_relation(relation_config)
        assert result.partitioned_by is None
        assert result.purge_mode is None

    def test_none_config_attr(self):
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = None
        result = integration.build_relation(relation_config)
        assert result.partitioned_by is None
        assert result.purge_mode is None


# ===================================================================
# Adapter registration
# ===================================================================

class TestAdapterCatalogRegistration:
    def test_only_datalake_registered(self):
        from dbt.adapters.teradata.impl import TeradataAdapter
        assert len(TeradataAdapter.CATALOG_INTEGRATIONS) == 1
        assert TeradataAdapter.CATALOG_INTEGRATIONS[0] is TeradataDatalakeCatalogIntegration


# ===================================================================
# TeradataRelation.render() -- 2-part native vs 3-part OTF
# ===================================================================

class TestTeradataRelationRender:
    """Exercise render() directly to assert the actual rendered string.

    Replaces the old TestThreeDotNaming, which only re-implemented the Jinja
    formula in Python and so caught nothing.
    """

    # -- 2-part native --

    def test_native_render_two_part(self):
        rel = TeradataRelation.create(schema="mydb", identifier="mytbl")
        assert rel.render() == '"mydb"."mytbl"'

    def test_native_render_database_none_schema_set(self):
        rel = TeradataRelation.create(database=None, schema="db", identifier="t")
        assert rel.render() == '"db"."t"'

    # -- 3-part OTF (explicit is_otf=True via create()) --

    def test_otf_render_three_part(self):
        rel = TeradataRelation.create(
            database="dl",
            schema="db",
            identifier="t",
            quote_policy={"database": False, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        assert rel.render() == 'dl."db"."t"'

    def test_otf_render_with_underscores_and_hyphens(self):
        rel = TeradataRelation.create(
            database="dl_1",
            schema="db-2",
            identifier="tbl 3",
            quote_policy={"database": False, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        assert rel.render() == 'dl_1."db-2"."tbl 3"'

    # -- Invariant guard: is_otf=True with a None part must raise --

    def test_otf_render_missing_database_raises(self):
        rel = TeradataRelation.create(
            database=None, schema="db", identifier="t",
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        with pytest.raises(DbtRuntimeError, match="OTF relation is missing"):
            rel.render()

    def test_otf_render_missing_schema_raises(self):
        rel = TeradataRelation.create(
            database="dl", schema=None, identifier="t",
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        with pytest.raises(DbtRuntimeError, match="OTF relation is missing"):
            rel.render()

    def test_otf_render_missing_identifier_returns_two_part(self):
        rel = TeradataRelation.create(
            database="dl", schema="db", identifier=None,
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        # Schema-only OTF relation (e.g. cache warming) returns 2-part form
        assert rel.render() == 'dl."db"'
