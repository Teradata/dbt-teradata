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
from dbt_common.exceptions import DbtRuntimeError, DbtDatabaseError

from dbt.adapters.teradata.catalogs import (
    TeradataDatalakeCatalogIntegration,
    TeradataCatalogRelation,
)
from dbt.adapters.teradata.relation import TeradataRelation
from dbt.adapters.teradata.impl import TeradataAdapter


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
    def test_datalake_registered(self):
        from dbt.adapters.teradata.impl import TeradataAdapter
        assert (
            TeradataDatalakeCatalogIntegration
            in TeradataAdapter.CATALOG_INTEGRATIONS
        )


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
            quote_policy={"database": True, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        assert rel.render() == '"dl"."db"."t"'

    def test_otf_render_with_underscores_and_hyphens(self):
        rel = TeradataRelation.create(
            database="dl_1",
            schema="db-2",
            identifier="tbl 3",
            quote_policy={"database": True, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        assert rel.render() == '"dl_1"."db-2"."tbl 3"'

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
        assert rel.render() == '"dl"."db"'

    # -- Additional OTF naming scenarios --

    def test_otf_render_long_identifiers(self):
        """OTF with long database, schema, and table names."""
        rel = TeradataRelation.create(
            database="my_production_datalake_us_east_1",
            schema="analytics_warehouse_iceberg_db",
            identifier="fact_orders_partitioned_by_date_2024",
            quote_policy={"database": True, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        assert rel.render() == (
            '"my_production_datalake_us_east_1".'
            '"analytics_warehouse_iceberg_db".'
            '"fact_orders_partitioned_by_date_2024"'
        )

    def test_otf_render_numeric_start_identifiers(self):
        """OTF names starting with numbers (valid when quoted)."""
        rel = TeradataRelation.create(
            database="datalake1",
            schema="123_db",
            identifier="456_table",
            quote_policy={"database": True, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        assert rel.render() == '"datalake1"."123_db"."456_table"'

    def test_otf_render_special_characters_in_schema(self):
        """OTF schema with dots and special chars (quoted)."""
        rel = TeradataRelation.create(
            database="dl",
            schema="my.otf.db",
            identifier="my_table",
            quote_policy={"database": True, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        assert rel.render() == '"dl"."my.otf.db"."my_table"'

    def test_otf_render_uppercase_datalake(self):
        """OTF datalake name in uppercase (quoted)."""
        rel = TeradataRelation.create(
            database="MY_DATALAKE",
            schema="iceberg_db",
            identifier="orders",
            quote_policy={"database": True, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        assert rel.render() == '"MY_DATALAKE"."iceberg_db"."orders"'

    def test_otf_render_doubles_embedded_quote(self):
        """Embedded double-quotes are doubled in each part.

        This locks the quoting contract that the Jinja helper
        teradata__quote_otf_part / teradata__build_otf_relation_name mirrors,
        so DDL/DML macros and relation rendering produce identical names.
        """
        rel = TeradataRelation.create(
            database='d"l',
            schema='o"db',
            identifier='t"bl',
            quote_policy={"database": True, "schema": True, "identifier": True},
            include_policy={"database": True, "schema": True, "identifier": True},
            is_otf=True,
        )
        assert rel.render() == '"d""l"."o""db"."t""bl"'


# ===================================================================
# TeradataRelation.create_from() -- OTF detection via catalog_name
# ===================================================================

class TestTeradataRelationCreateFromOTF:
    """Verify create_from() detects catalog_name, calls
    adapter.get_catalog_integration(), and builds an OTF 3-part relation.
    """

    def test_create_from_catalog_name_builds_otf_relation(self, monkeypatch):
        from dbt.adapters import factory as adapter_factory

        cat = MagicMock(catalog_type="datalake", datalake_name="dl", otf_database="db")
        adapter = MagicMock()
        adapter.get_catalog_integration.return_value = cat
        monkeypatch.setattr(adapter_factory, "get_adapter", lambda _: adapter)

        rc = MagicMock(identifier="tbl", config={"catalog_name": "test_catalog"})
        rel = TeradataRelation.create_from(MagicMock(), rc, type="table")

        assert rel.is_otf is True
        assert rel.render() == '"dl"."db"."tbl"'


# ===================================================================
# Build relation -- additional DDL config scenarios
# ===================================================================

class TestBuildRelationDDLCombinations:
    """Verify build_relation() handles various combinations of DDL configs
    from model config matching OTF PDF test scenarios.
    """

    def _make_integration(self, **config_overrides):
        return TeradataDatalakeCatalogIntegration(_make_valid_config(**config_overrides))

    def test_partition_with_transform_functions(self):
        """Covers: partition transforms (bucket, truncate, month, year)."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "partitioned_by": "YEAR(order_date), BUCKET(16, customer_id)",
        }
        result = integration.build_relation(relation_config)
        assert result.partitioned_by == "YEAR(order_date), BUCKET(16, customer_id)"

    def test_partition_with_month_transform(self):
        """Covers: MONTH partition transform from Native OTF scenarios."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "partitioned_by": "MONTH(created_at)",
        }
        result = integration.build_relation(relation_config)
        assert result.partitioned_by == "MONTH(created_at)"

    def test_partition_with_truncate_transform(self):
        """Covers: TRUNCATE partition transform."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "partitioned_by": "TRUNCATE(10, product_name)",
        }
        result = integration.build_relation(relation_config)
        assert result.partitioned_by == "TRUNCATE(10, product_name)"

    def test_all_ddl_options_combined(self):
        """Covers: Managed OTF Phase-1 combinations (PI+PB+SB)."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "partitioned_by": "YEAR(dt), country",
            "sorted_by": "id ASC, created_at DESC",
            "tblproperties": "'write.format.default'='parquet', 'gc.enabled'='true'",
            "purge_mode": "PURGE ALL",
        }
        result = integration.build_relation(relation_config)
        assert result.partitioned_by == "YEAR(dt), country"
        assert result.sorted_by == "id ASC, created_at DESC"
        assert "'write.format.default'='parquet'" in result.tblproperties
        assert "'gc.enabled'='true'" in result.tblproperties
        assert result.purge_mode == "PURGE ALL"

    def test_tblproperties_write_format_orc(self):
        """Covers: OTF write in ORC format via tblproperties."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "tblproperties": "'write.format.default'='orc'",
        }
        result = integration.build_relation(relation_config)
        assert result.tblproperties == "'write.format.default'='orc'"

    def test_tblproperties_write_format_avro(self):
        """Covers: OTF write in Avro format via tblproperties."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "tblproperties": "'write.format.default'='avro'",
        }
        result = integration.build_relation(relation_config)
        assert result.tblproperties == "'write.format.default'='avro'"

    def test_tblproperties_catalog_database_name(self):
        """Covers: Managed OTF external catalog name mapping."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "tblproperties": "'CATALOG_Database_NAME'='ext_db', 'CATALOG_Table_NAME'='ext_tbl'",
        }
        result = integration.build_relation(relation_config)
        assert "CATALOG_Database_NAME" in result.tblproperties
        assert "CATALOG_Table_NAME" in result.tblproperties

    def test_tblproperties_compression_snappy(self):
        """Covers: Managed OTF Phase-1 compression (snappy only in Phase-1)."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "tblproperties": "'write.parquet.compression-codec'='snappy'",
        }
        result = integration.build_relation(relation_config)
        assert "snappy" in result.tblproperties

    def test_sorted_by_multiple_columns(self):
        """Covers: SORTED BY with multiple columns."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "sorted_by": "region ASC, order_date DESC, customer_id ASC",
        }
        result = integration.build_relation(relation_config)
        assert result.sorted_by == "region ASC, order_date DESC, customer_id ASC"

    def test_config_keys_case_sensitivity(self):
        """Verify config keys must match exactly (case sensitive per MOTF spec)."""
        integration = self._make_integration()
        relation_config = MagicMock()
        relation_config.config = {
            "partitioned_by": "col1",
            "sorted_by": "col2 ASC",
        }
        result = integration.build_relation(relation_config)
        assert result.partitioned_by == "col1"
        assert result.sorted_by == "col2 ASC"
        # Uppercase keys should not be picked up
        relation_config2 = MagicMock()
        relation_config2.config = {
            "PARTITIONED_BY": "col1",
        }
        result2 = integration.build_relation(relation_config2)
        assert result2.partitioned_by is None


# ===================================================================
# Multiple catalog integrations
# ===================================================================

class TestMultipleCatalogIntegrations:
    """Verify that multiple catalog integrations can coexist with
    different datalake/otf_database configurations (multi-catalog support).
    """

    def test_two_catalogs_different_datalakes(self):
        """Covers: Hive Catalog + Glue Catalog in same project."""
        config_glue = _make_config(
            name="glue_catalog",
            adapter_properties={
                "datalake_name": "aws_glue_datalake",
                "otf_database": "glue_iceberg_db",
            },
        )
        config_unity = _make_config(
            name="unity_catalog",
            adapter_properties={
                "datalake_name": "azure_unity_datalake",
                "otf_database": "unity_iceberg_db",
            },
        )
        glue_integration = TeradataDatalakeCatalogIntegration(config_glue)
        unity_integration = TeradataDatalakeCatalogIntegration(config_unity)

        assert glue_integration.datalake_name == "aws_glue_datalake"
        assert glue_integration.otf_database == "glue_iceberg_db"
        assert unity_integration.datalake_name == "azure_unity_datalake"
        assert unity_integration.otf_database == "unity_iceberg_db"

    def test_two_catalogs_build_different_relations(self):
        """Each catalog integration produces its own 3-part naming."""
        config_primary = _make_config(
            name="primary",
            adapter_properties={
                "datalake_name": "dl_primary",
                "otf_database": "db_primary",
            },
        )
        config_secondary = _make_config(
            name="secondary",
            adapter_properties={
                "datalake_name": "dl_secondary",
                "otf_database": "db_secondary",
            },
        )
        primary = TeradataDatalakeCatalogIntegration(config_primary)
        secondary = TeradataDatalakeCatalogIntegration(config_secondary)

        relation_config = MagicMock()
        relation_config.config = {"partitioned_by": "region"}

        rel_p = primary.build_relation(relation_config)
        rel_s = secondary.build_relation(relation_config)

        assert rel_p.datalake_name == "dl_primary"
        assert rel_p.otf_database == "db_primary"
        assert rel_s.datalake_name == "dl_secondary"
        assert rel_s.otf_database == "db_secondary"
        # Both pass through the same model config
        assert rel_p.partitioned_by == "region"
        assert rel_s.partitioned_by == "region"


# ===================================================================
# TeradataAdapter.otf_relation_exists -- error handling
# ===================================================================

class TestOtfRelationExists:
    """Unit tests for TeradataAdapter.otf_relation_exists().

    The method is tested by calling the unbound implementation on a
    minimal MagicMock self so no database connection is required.
    """

    def _call(self, mock_self):
        mock_self.Relation = TeradataRelation
        return TeradataAdapter.otf_relation_exists(
            mock_self, "my_datalake", "my_otf_db", "my_table"
        )

    def test_returns_true_when_execute_succeeds(self):
        """Table exists: execute() returns normally → True."""
        mock_self = MagicMock()
        mock_self.connections.execute.return_value = None
        assert self._call(mock_self) is True

    def test_executes_correct_3part_quoted_sql(self):
        """Verify the SAMPLE 0 probe uses the properly quoted 3-part OTF name."""
        mock_self = MagicMock()
        mock_self.connections.execute.return_value = None
        self._call(mock_self)
        executed_sql = mock_self.connections.execute.call_args[0][0]
        assert '"my_datalake"."my_otf_db"."my_table"' in executed_sql
        assert "SAMPLE 0" in executed_sql

    def test_returns_false_for_error_7825(self):
        """Table not found: DbtDatabaseError with [Error 7825] → False."""
        mock_self = MagicMock()
        mock_self.connections.execute.side_effect = DbtDatabaseError(
            "[Error 7825] ICEBERG_EXPORT: Table does not exist"
        )
        assert self._call(mock_self) is False

    def test_reraises_other_dbt_database_errors(self):
        """Auth/permission errors must propagate, not be swallowed."""
        mock_self = MagicMock()
        mock_self.connections.execute.side_effect = DbtDatabaseError(
            "[Error 3524] The user does not have SELECT access to my_table"
        )
        with pytest.raises(DbtDatabaseError, match="3524"):
            self._call(mock_self)

    def test_reraises_non_database_exceptions(self):
        """Network / unexpected errors must propagate unchanged."""
        mock_self = MagicMock()
        mock_self.connections.execute.side_effect = RuntimeError("connection reset")
        with pytest.raises(RuntimeError, match="connection reset"):
            self._call(mock_self)
