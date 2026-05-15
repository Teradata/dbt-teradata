"""Catalog integrations for Teradata Open Table Format (OTF) support.

This module provides the DATALAKE catalog integration that enables dbt to
create and manage Iceberg/Delta Lake tables via Teradata's native OTF support.
Tables are addressed using 3-part naming: <datalake>."<otf_database>"."<table>".

Configuration in catalogs.yml:

    catalogs:
      - name: my_catalog
        write_integrations:
          - name: iceberg_glue
            catalog_type: datalake
            adapter_properties:
              datalake_name: MyOTFLake        # pre-created DATALAKE object
              otf_database: my_otf_db         # pre-created database within the DATALAKE

Model-level config options (set via {{ config(...) }} in .sql files):

    partitioned_by  -- Iceberg partition expression, e.g. 'YEAR(dt), country'
    sorted_by       -- Iceberg sort order, e.g. 'id ASC'
    tblproperties   -- Iceberg table properties, e.g. "'gc.enabled'='true'"
    purge_mode      -- DROP behavior: 'PURGE ALL' (default) or 'NO PURGE'
                       PURGE ALL  = remove catalog entry + delete data files
                       NO PURGE   = remove catalog entry, keep data files on object store
"""

from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
    CatalogRelation,
    InvalidCatalogIntegrationConfigError,
)
from dbt.adapters.contracts.relation import RelationConfig


# ---------------------------------------------------------------------------
# Relation dataclass -- carries catalog metadata into Jinja macros
# ---------------------------------------------------------------------------

@dataclass
class TeradataCatalogRelation(CatalogRelation):
    """Relation metadata for DATALAKE-based OTF tables.

    Fields populated from catalogs.yml (via the integration):
        catalog_type, catalog_name, table_format, file_format,
        external_volume, datalake_name, otf_database

    Fields populated from model config (via build_relation):
        partitioned_by, sorted_by, tblproperties, purge_mode
    """
    catalog_type: Optional[str] = None
    catalog_name: Optional[str] = None
    table_format: Optional[str] = None
    file_format: Optional[str] = None
    external_volume: Optional[str] = None
    datalake_name: Optional[str] = None
    otf_database: Optional[str] = None
    partitioned_by: Optional[str] = None
    sorted_by: Optional[str] = None
    tblproperties: Optional[str] = None
    # Controls DROP TABLE behavior for OTF tables (per Teradata DFES spec):
    #   'PURGE ALL' -- removes catalog entry AND deletes data files (default)
    #   'NO PURGE'  -- removes catalog entry only, data files remain on object store
    purge_mode: Optional[str] = None


# ---------------------------------------------------------------------------
# DATALAKE integration -- 3-part naming (datalake."otf_db"."table")
# ---------------------------------------------------------------------------

class TeradataDatalakeCatalogIntegration(CatalogIntegration):
    """Catalog integration for Teradata DATALAKE objects.

    Works with any external catalog (AWS Glue, Unity Catalog, etc.) because
    Teradata's DATALAKE object encapsulates the catalog type, auth, and
    object store path. dbt only needs the datalake_name and otf_database
    for 3-part naming.

    Generated SQL example:
        DROP TABLE  /*+ IF EXISTS */ <datalake>."<otf_db>"."<table>" PURGE ALL;
        CREATE TABLE <datalake>."<otf_db>"."<table>"
            PARTITIONED BY (...)
            SORTED BY ...
            TBLPROPERTIES(...)
            AS (...) WITH DATA;

    Required adapter_properties in catalogs.yml:
        datalake_name  -- name of the pre-created DATALAKE object in Teradata
        otf_database   -- name of the pre-created database within the DATALAKE
    """

    catalog_type = "datalake"
    allows_writes = True
    table_format = "iceberg"
    file_format = "parquet"

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)
        # Restore class-level defaults if not provided in config
        if config.file_format is None:
            self.file_format = "parquet"
        adapter_props = config.adapter_properties or {}
        self.datalake_name = adapter_props.get("datalake_name")
        self.otf_database = adapter_props.get("otf_database")
        if not self.datalake_name:
            raise InvalidCatalogIntegrationConfigError(
                config.name,
                "adapter_properties.datalake_name is required -- "
                "it must match the pre-created DATALAKE object in Teradata",
            )
        if not self.otf_database:
            raise InvalidCatalogIntegrationConfigError(
                config.name,
                "adapter_properties.otf_database is required -- "
                "it must match the pre-created OTF database within the DATALAKE",
            )

    def build_relation(self, config: RelationConfig) -> TeradataCatalogRelation:
        """Build a TeradataCatalogRelation from integration + model config.

        Integration-level fields (datalake_name, otf_database, etc.) come from
        catalogs.yml. Model-level fields (partitioned_by, sorted_by,
        tblproperties, purge_mode) come from the model's {{ config(...) }}.
        """
        model_config = config.config if hasattr(config, 'config') and config.config else {}
        return TeradataCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.catalog_name,
            table_format=self.table_format,
            file_format=self.file_format,
            external_volume=self.external_volume,
            datalake_name=self.datalake_name,
            otf_database=self.otf_database,
            partitioned_by=model_config.get("partitioned_by"),
            sorted_by=model_config.get("sorted_by"),
            tblproperties=model_config.get("tblproperties"),
            purge_mode=model_config.get("purge_mode"),
        )
