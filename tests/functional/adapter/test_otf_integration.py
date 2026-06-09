"""End-to-end functional tests for OTF (Iceberg/Delta Lake) materialization.

These tests require a Teradata instance with a pre-created DATALAKE object
and OTF database. They are gated on env vars and skipped otherwise so the
file is safe to include in the standard pytest run.

Required env vars:
  DBT_TERADATA_DATALAKE       -- name of the pre-created DATALAKE object
  DBT_TERADATA_OTF_DATABASE   -- name of the pre-created OTF database within
                                  that DATALAKE

The standard DBT_TERADATA_* connection env vars (or their defaults from
tests/conftest.py) are used for the Teradata connection.

Scenarios covered:
  1.  Basic OTF table create
  2.  Idempotency (DROP+CREATE cycle survives re-runs)
  3.  Cross-model ref() to an OTF model produces 3-part compiled SQL
  4.  Cross-model source() with database/schema set produces 3-part SQL
      (verifies the database != schema auto-OTF heuristic)
  5.  purge_mode: 'NO PURGE' end-to-end
  6.  PARTITIONED BY with transform functions (YEAR, BUCKET)
  7.  SORTED BY with multiple columns
  8.  All DDL options combined (partition + sort + tblproperties + purge)
  9.  View materialization reading from OTF source (compile check)
  10. CTAS from OTF to native table (cross-ref compile check)
  11. CTAS from native to OTF (cross-ref compile check)
  12. OTF model with sql_header config
  13. PURGE ALL end-to-end
  14. Multiple file formats via tblproperties
  15. OTF incremental append: first run creates, second run appends (proves
      is_incremental() fired via source-row deletion + survival assertion)
  16. OTF incremental append with PARTITIONED BY (same incremental proof)
  17. OTF incremental with --full-refresh (non-alias)
  18. dbt `alias` config on OTF table materialization
  19. dbt `alias` config on OTF incremental: create + true append proof
  20. dbt `alias` config + --full-refresh on OTF incremental
  21. Long alias name (64 chars): identifier plumbing does not truncate or
      mangle the alias; staging table name stays within Teradata limits
  22. ref() to an aliased OTF incremental model compiles to the alias-named
      3-part DATALAKE identifier (compile-only check)
  23. dbt `alias` + `partitioned_by` on OTF incremental: create + true append
      proof across two year-partitions
"""

import os

import pytest

from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt


DATALAKE_NAME = os.getenv("DBT_TERADATA_DATALAKE")
OTF_DATABASE = os.getenv("DBT_TERADATA_OTF_DATABASE")

# 64-character alias used by TestOTFLongAlias (Scenario 21).
# Staging name = OTF_LONG_ALIAS_NAME + "__dbt_tmp" = 73 chars — within
# Teradata's 128-char identifier limit and well under OTF catalog limits.
OTF_LONG_ALIAS_NAME = "otf_long_alias_sixty_four_chars_to_test_identifier_length_limits"

pytestmark = pytest.mark.skipif(
    not (DATALAKE_NAME and OTF_DATABASE),
    reason="requires DBT_TERADATA_DATALAKE and DBT_TERADATA_OTF_DATABASE env vars",
)

# OTF tables are created in the shared external OTF database (not the per-test
# `{schema}`), so they are not cleaned up by the standard dbt test harness.
_OTF_TEST_TABLES = [
    "basic_otf",
    "otf_no_purge",
    "otf_partitioned_transforms",
    "otf_partitioned_month",
    "otf_sorted_multi",
    "otf_all_options",
    "otf_base_for_view",
    "otf_source_model",
    "otf_with_header",
    "otf_purge_all",
    "otf_write_orc",
    "otf_write_avro",
    "otf_write_parquet_gzip",
    "otf_inc_append",
    "otf_inc_append_partitioned",
    "otf_inc_full_refresh",
    # dbt `alias` config: physical OTF objects carry the *alias*, not the
    # model file name, so register the alias names for cleanup (not the file
    # names otf_alias_model / otf_alias_inc).
    "otf_aliased_object",
    "otf_alias_inc_object",
    # Scenario 20: --full-refresh with alias
    "otf_alias_fr_object",
    # Scenario 21: long alias
    OTF_LONG_ALIAS_NAME,
    # Scenario 23: alias + partitioned_by incremental
    "otf_alias_partitioned_object",
]


def _is_otf_table_not_found(exc: Exception) -> bool:
    """True if the exception is an OTF 'table does not exist' error.

    project.run_sql() calls cursor.execute() directly, bypassing the adapter's
    add_query() where /*+ IF EXISTS */ suppression lives.  Error 7825 (older
    engines) and 6321 (TD 20.0.0.61+) must be swallowed during cleanup so that
    one missing table does not abort cleanup of the rest.
    """
    msg = str(exc)
    return "[Error 7825]" in msg or "[Error 6321]" in msg


def _drop_otf_test_tables(project) -> None:
    for name in _OTF_TEST_TABLES:
        try:
            project.run_sql(
                f'DROP TABLE /*+ IF EXISTS */ "{DATALAKE_NAME}"."{OTF_DATABASE}"."{name}" NO PURGE;'
            )
        except Exception as exc:
            if not _is_otf_table_not_found(exc):
                raise


@pytest.fixture(autouse=True)
def _cleanup_otf_tables(project):
    _drop_otf_test_tables(project)  # pre-test: clear any leftovers from prior runs
    yield
    _drop_otf_test_tables(project)  # post-test: clean up what this test created


CATALOG_NAME = "test_catalog"

CATALOGS_CONFIG = {
    "catalogs": [
        {
            "name": CATALOG_NAME,
            "active_write_integration": "td_datalake",
            "write_integrations": [
                {
                    "name": "td_datalake",
                    "catalog_type": "datalake",
                    "adapter_properties": {
                        "datalake_name": DATALAKE_NAME,
                        "otf_database": OTF_DATABASE,
                    },
                }
            ],
        }
    ]
}


# ---------------------------------------------------------------------------
# Model SQL fixtures
# ---------------------------------------------------------------------------

basic_otf_model_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_src
"""

otf_with_purge_mode_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    purge_mode='NO PURGE'
) }}}}
select id from {{{{ target.schema }}}}.otf_src
"""

downstream_of_otf_sql = """
{{ config(materialized='table') }}
select * from {{ ref('basic_otf') }}
"""

source_referencing_otf_sql = """
{{ config(materialized='table') }}
select * from {{ source('otf_source', 'external_otf_table') }}
"""

sources_yml = f"""
version: 2
sources:
  - name: otf_source
    database: {DATALAKE_NAME or 'placeholder'}
    schema: {OTF_DATABASE or 'placeholder'}
    tables:
      - name: external_otf_table
"""


# ===================================================================
# Scenarios 1 & 2: basic create + idempotency
# ===================================================================

class TestOTFBasicAndIdempotent(BaseCatalogIntegrationValidation):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"basic_otf.sql": basic_otf_model_sql}

    def test_basic_create_and_rerun(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_src VALUES (1, 'a')")
        project.run_sql("INSERT INTO {schema}.otf_src VALUES (2, 'b')")
        try:
            # First run: creates the OTF table.
            results = run_dbt(["run", "--select", "basic_otf"])
            assert len(results) == 1
            assert results[0].status == "success"

            # Second run: must succeed idempotently (DROP+CREATE cycle).
            results = run_dbt(["run", "--select", "basic_otf"])
            assert len(results) == 1
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src")


# ===================================================================
# Scenario 3: cross-model ref() produces 3-part name in compiled SQL
# ===================================================================

class TestOTFRefRendersThreePartName(BaseCatalogIntegrationValidation):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_otf.sql": basic_otf_model_sql,
            "downstream_of_otf.sql": downstream_of_otf_sql,
        }

    def test_compiled_sql_contains_three_part_name(self, project):
        run_dbt(["compile", "--select", "downstream_of_otf"])
        compiled_path = os.path.join(
            str(project.project_root),
            "target", "compiled", "test", "models", "downstream_of_otf.sql",
        )
        with open(compiled_path, "r", encoding="utf-8") as f:
            compiled = f.read()
        # 3-part: "<datalake>"."<otf_db>"."<table>"
        expected = f'"{DATALAKE_NAME}"."{OTF_DATABASE}"."basic_otf"'
        assert expected in compiled, (
            f"Expected 3-part OTF name {expected!r} in compiled SQL, got:\n{compiled}"
        )


# ===================================================================
# Scenario 4: source() with database != schema triggers OTF heuristic
# ===================================================================

class TestOTFSourceHeuristic(BaseCatalogIntegrationValidation):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"source_referencing_otf.sql": source_referencing_otf_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"sources.yml": sources_yml}

    def test_source_compiles_to_three_part_name(self, project):
        # Compile only -- we don't require the source to actually exist.
        run_dbt(["compile", "--select", "source_referencing_otf"])
        compiled_path = os.path.join(
            str(project.project_root),
            "target", "compiled", "test", "models", "source_referencing_otf.sql",
        )
        with open(compiled_path, "r", encoding="utf-8") as f:
            compiled = f.read()
        expected = f'"{DATALAKE_NAME}"."{OTF_DATABASE}"."external_otf_table"'
        assert expected in compiled, (
            f"Expected 3-part OTF source name {expected!r} in compiled SQL, "
            f"got:\n{compiled}"
        )


# ===================================================================
# Scenario 5: purge_mode: 'NO PURGE' end-to-end
# ===================================================================

class TestOTFNoPurge(BaseCatalogIntegrationValidation):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_no_purge.sql": otf_with_purge_mode_sql}

    def test_no_purge_run_succeeds(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_src VALUES (1, 'a')")
        try:
            results = run_dbt(["run", "--select", "otf_no_purge"])
            assert len(results) == 1
            assert results[0].status == "success"
            # Re-run to exercise the DROP path with NO PURGE.
            results = run_dbt(["run", "--select", "otf_no_purge"])
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src")


# ===================================================================
# Scenario 6: PARTITIONED BY with transform functions
# ===================================================================

otf_partitioned_transforms_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    partitioned_by='YEAR(order_date), BUCKET(16, customer_id)'
) }}}}
select id, order_date, customer_id, amount from {{{{ target.schema }}}}.otf_src_orders
"""

otf_partitioned_month_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    partitioned_by='MONTH(created_at)'
) }}}}
select id, created_at from {{{{ target.schema }}}}.otf_src_orders
"""


class TestOTFPartitionTransforms(BaseCatalogIntegrationValidation):
    """Verify PARTITIONED BY with transform functions like YEAR, BUCKET, MONTH.
    Exercises partition transforms on the External OTF (CREATE TABLE) path.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "otf_partitioned_transforms.sql": otf_partitioned_transforms_sql,
            "otf_partitioned_month.sql": otf_partitioned_month_sql,
        }

    def test_partition_transforms_run_succeeds(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src_orders "
            "(id INTEGER, order_date DATE, customer_id INTEGER, "
            "amount DECIMAL(10,2), created_at TIMESTAMP)"
        )
        project.run_sql(
            "INSERT INTO {schema}.otf_src_orders "
            "VALUES (1, DATE '2024-03-15', 100, 99.99, CURRENT_TIMESTAMP)"
        )
        try:
            results = run_dbt(["run", "--select", "otf_partitioned_transforms"])
            assert len(results) == 1
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src_orders")

    def test_partition_month_run_succeeds(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src_orders "
            "(id INTEGER, order_date DATE, customer_id INTEGER, "
            "amount DECIMAL(10,2), created_at TIMESTAMP)"
        )
        project.run_sql(
            "INSERT INTO {schema}.otf_src_orders "
            "VALUES (1, DATE '2024-06-01', 200, 50.00, CURRENT_TIMESTAMP)"
        )
        try:
            results = run_dbt(["run", "--select", "otf_partitioned_month"])
            assert len(results) == 1
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src_orders")


# ===================================================================
# Scenario 7: SORTED BY with multiple columns
# ===================================================================

otf_sorted_multi_col_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    sorted_by='region ASC, order_date DESC, customer_id ASC'
) }}}}
select id, region, order_date, customer_id
from {{{{ target.schema }}}}.otf_src_orders
"""


class TestOTFSortedByMultiColumn(BaseCatalogIntegrationValidation):
    """Verify SORTED BY with multiple columns and directions on the External
    OTF (CREATE TABLE) path.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_sorted_multi.sql": otf_sorted_multi_col_sql}

    def test_sorted_by_multi_column_succeeds(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src_orders "
            "(id INTEGER, region VARCHAR(50), order_date DATE, customer_id INTEGER)"
        )
        project.run_sql(
            "INSERT INTO {schema}.otf_src_orders "
            "VALUES (1, 'US-EAST', DATE '2024-01-01', 100)"
        )
        try:
            results = run_dbt(["run", "--select", "otf_sorted_multi"])
            assert len(results) == 1
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src_orders")


# ===================================================================
# Scenario 8: All DDL options combined
# ===================================================================

otf_all_options_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    partitioned_by='YEAR(order_date)',
    sorted_by='customer_id ASC',
    tblproperties="'write.format.default'='parquet', 'gc.enabled'='true'",
    purge_mode='NO PURGE'
) }}}}
select id, order_date, customer_id, amount
from {{{{ target.schema }}}}.otf_src_orders
"""


class TestOTFAllDDLOptions(BaseCatalogIntegrationValidation):
    """Verify combined DDL options in a single External OTF model.

    Exercises PARTITIONED BY + SORTED BY + TBLPROPERTIES together on the
    External OTF (CREATE TABLE) path. Note: this does NOT cover primary-index
    combinations -- INDEX is not allowed for External OTF, and the model below
    sets no index. Index/PI support is a Managed OTF concern tracked separately.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_all_options.sql": otf_all_options_sql}

    def test_all_ddl_options_run_succeeds(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src_orders "
            "(id INTEGER, order_date DATE, customer_id INTEGER, "
            "amount DECIMAL(10,2))"
        )
        project.run_sql(
            "INSERT INTO {schema}.otf_src_orders "
            "VALUES (1, DATE '2024-07-04', 42, 199.99)"
        )
        try:
            results = run_dbt(["run", "--select", "otf_all_options"])
            assert len(results) == 1
            assert results[0].status == "success"

            # Idempotency: second run also succeeds
            results = run_dbt(["run", "--select", "otf_all_options"])
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src_orders")


# ===================================================================
# Scenario 9: View materialization reading from OTF source
# ===================================================================

otf_base_for_view_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_src
"""

view_from_otf_sql = """
{{ config(materialized='view') }}
select id, name from {{ ref('otf_base_for_view') }}
"""


class TestOTFViewFromOTFSource(BaseCatalogIntegrationValidation):
    """Verify a view materialization can reference an OTF model.
    The compiled SQL should contain the 3-part OTF name in the view definition.
    Covers: Native OTF scenario #20 (Create Views reading from OTF tables).
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "otf_base_for_view.sql": otf_base_for_view_sql,
            "view_from_otf.sql": view_from_otf_sql,
        }

    def test_view_from_otf_compiles_with_three_part_name(self, project):
        run_dbt(["compile", "--select", "view_from_otf"])
        compiled_path = os.path.join(
            str(project.project_root),
            "target", "compiled", "test", "models", "view_from_otf.sql",
        )
        with open(compiled_path, "r", encoding="utf-8") as f:
            compiled = f.read()
        expected = f'"{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_base_for_view"'
        assert expected in compiled, (
            f"Expected 3-part OTF name {expected!r} in compiled view SQL, "
            f"got:\n{compiled}"
        )


# ===================================================================
# Scenario 10: CTAS from OTF to native table (cross-ref)
# ===================================================================

otf_source_model_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_src
"""

native_from_otf_ref_sql = """
{{ config(materialized='table') }}
select id, name from {{ ref('otf_source_model') }}
"""


class TestOTFCrossRefOTFToNative(BaseCatalogIntegrationValidation):
    """Verify a native table can select from an OTF model via ref().
    Compiled SQL should use 3-part name for the OTF source.
    Exercises CREATE TABLE ... AS with an External OTF table as source.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "otf_source_model.sql": otf_source_model_sql,
            "native_from_otf_ref.sql": native_from_otf_ref_sql,
        }

    def test_native_ctas_from_otf_compiles(self, project):
        run_dbt(["compile", "--select", "native_from_otf_ref"])
        compiled_path = os.path.join(
            str(project.project_root),
            "target", "compiled", "test", "models", "native_from_otf_ref.sql",
        )
        with open(compiled_path, "r", encoding="utf-8") as f:
            compiled = f.read()
        expected = f'"{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_source_model"'
        assert expected in compiled, (
            f"Expected 3-part OTF ref {expected!r} in native model SQL, "
            f"got:\n{compiled}"
        )


# ===================================================================
# Scenario 11: CTAS from native to OTF (cross-ref)
# ===================================================================

native_source_model_sql = """
{{ config(materialized='table') }}
select 1 as id, 'product_a' as name
"""

otf_from_native_ref_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}'
) }}}}
select id, name from {{{{ ref('native_source_model') }}}}
"""


class TestOTFCrossRefNativeToOTF(BaseCatalogIntegrationValidation):
    """Verify an OTF model can select from a native model via ref().
    The compiled SQL for the OTF model should reference the native table
    using standard 2-part naming (schema.table).
    Covers: Hive Catalog scenario #4 (CTAS from BFS/OFS into OTF).
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "native_source_model.sql": native_source_model_sql,
            "otf_from_native_ref.sql": otf_from_native_ref_sql,
        }

    def test_otf_ctas_from_native_compiles(self, project):
        run_dbt(["compile", "--select", "otf_from_native_ref"])
        compiled_path = os.path.join(
            str(project.project_root),
            "target", "compiled", "test", "models", "otf_from_native_ref.sql",
        )
        with open(compiled_path, "r", encoding="utf-8") as f:
            compiled = f.read()
        # The native model should be referenced with 2-part name (schema.table)
        assert "native_source_model" in compiled
        # The OTF model target is handled by the macro, not in compiled SQL body


# ===================================================================
# Scenario 12: OTF model with sql_header config
# ===================================================================

otf_with_sql_header_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    sql_header='SET QUERY_BAND = \\'app=dbt;model=otf_header;\\' FOR SESSION;'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_src
"""


class TestOTFWithSqlHeader(BaseCatalogIntegrationValidation):
    """Verify OTF models support sql_header config.
    Covers: custom session settings before OTF DDL execution.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_with_header.sql": otf_with_sql_header_sql}

    def test_otf_with_sql_header_succeeds(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_src VALUES (1, 'header_test')")
        try:
            results = run_dbt(["run", "--select", "otf_with_header"])
            assert len(results) == 1
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src")


# ===================================================================
# Scenario 13: PURGE ALL end-to-end
# ===================================================================

otf_purge_all_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    purge_mode='PURGE ALL'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_src
"""


class TestOTFPurgeAll(BaseCatalogIntegrationValidation):
    """Verify purge_mode='PURGE ALL' works end-to-end.
    Covers: explicit PURGE ALL in DROP TABLE for OTF tables.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_purge_all.sql": otf_purge_all_sql}

    def test_purge_all_run_succeeds(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_src VALUES (1, 'purge_test')")
        try:
            # First run: create the table
            results = run_dbt(["run", "--select", "otf_purge_all"])
            assert len(results) == 1
            assert results[0].status == "success"
            # Second run: exercises DROP with PURGE ALL + re-create
            results = run_dbt(["run", "--select", "otf_purge_all"])
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src")


# ===================================================================
# Scenario 14: Different file formats via tblproperties
# ===================================================================

otf_write_orc_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    tblproperties="'write.format.default'='orc'"
) }}}}
select id, name from {{{{ target.schema }}}}.otf_src
"""

otf_write_avro_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    tblproperties="'write.format.default'='avro'"
) }}}}
select id, name from {{{{ target.schema }}}}.otf_src
"""

otf_write_parquet_gzip_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    tblproperties="'write.format.default'='parquet', 'write.parquet.compression-codec'='gzip'"
) }}}}
select id, name from {{{{ target.schema }}}}.otf_src
"""


class TestOTFFileFormats(BaseCatalogIntegrationValidation):
    """Verify write data-file format configuration via tblproperties.

    Avro and Parquet (incl. gzip compression) are supported write formats for
    External OTF (Iceberg). Writing ORC data files in Iceberg is NOT supported
    by Teradata OTF, so that case is asserted as a failure rather than a success.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "otf_write_orc.sql": otf_write_orc_sql,
            "otf_write_avro.sql": otf_write_avro_sql,
            "otf_write_parquet_gzip.sql": otf_write_parquet_gzip_sql,
        }

    def test_orc_format_fails(self, project):
        """Writing ORC data files in Iceberg is not supported, so materializing
        a model with write.format.default='orc' must fail with an ORC-specific
        error (e.g. TD_ICEBERG_WRITE: Cannot write ORC file)."""
        project.run_sql(
            "CREATE TABLE {schema}.otf_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_src VALUES (1, 'orc_test')")
        try:
            results = run_dbt(
                ["run", "--select", "otf_write_orc"], expect_pass=False
            )
            assert len(results) == 1
            assert results[0].status != "success"
            # Be specific: the failure must be about the unsupported ORC write,
            # not an unrelated error (e.g. connection/auth). The Teradata engine
            # reports "Cannot write ORC file" from TD_ICEBERG_WRITE.
            message = str(results[0].message or "").lower()
            assert "orc" in message, (
                "Expected an ORC-related failure message, got: "
                f"{results[0].message!r}"
            )
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src")

    def test_avro_format_succeeds(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_src VALUES (1, 'avro_test')")
        try:
            results = run_dbt(["run", "--select", "otf_write_avro"])
            assert len(results) == 1
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src")

    def test_parquet_gzip_format_succeeds(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_src VALUES (1, 'gzip_test')")
        try:
            results = run_dbt(["run", "--select", "otf_write_parquet_gzip"])
            assert len(results) == 1
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src")


# ===================================================================
# Scenario 15: OTF incremental append
# ===================================================================

otf_inc_append_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='append'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_inc_src

{{% if is_incremental() %}}
    where id > (select max(id) from {{{{ this }}}})
{{% endif %}}
"""


class TestOTFIncrementalAppend(BaseCatalogIntegrationValidation):
    """Verify OTF incremental append: first run creates, second run appends.
    Covers: append strategy (default) for OTF tables.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_inc_append.sql": otf_inc_append_sql}

    def test_incremental_append_first_and_second_run(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_inc_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_inc_src VALUES (1, 'alice')")
        project.run_sql("INSERT INTO {schema}.otf_inc_src VALUES (2, 'bob')")
        try:
            # First run: creates the OTF table via CREATE TABLE AS.
            results = run_dbt(["run", "--select", "otf_inc_append"])
            assert len(results) == 1
            assert results[0].status == "success"

            # Verify first run produced exactly 2 rows (alice, bob).
            stats = project.run_sql(
                f'SELECT MIN(id), MAX(id), COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_inc_append"',
                fetch="one",
            )
            assert stats[0] == 1
            assert stats[1] == 2
            assert stats[2] == 2

            # Delete id=1 from the source to prove the second run is truly
            # append-only (existing OTF rows must survive even if removed
            # from the source).
            project.run_sql("DELETE FROM {schema}.otf_inc_src WHERE id = 1")
            project.run_sql("INSERT INTO {schema}.otf_inc_src VALUES (3, 'charlie')")
            project.run_sql("INSERT INTO {schema}.otf_inc_src VALUES (4, 'diana')")

            # Second run: should only append new rows (id > max existing = 2).
            results = run_dbt(["run", "--select", "otf_inc_append"])
            assert len(results) == 1
            assert results[0].status == "success"

            # Verify: all 4 rows present (alice/bob from run 1 + charlie/diana
            # from run 2).  id=1 (alice) must still be present — it was deleted
            # from the source but append must never remove OTF rows.
            stats = project.run_sql(
                f'SELECT MIN(id), MAX(id), COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_inc_append"',
                fetch="one",
            )
            assert stats[0] == 1   # alice still present (append-only)
            assert stats[1] == 4   # diana is newest
            assert stats[2] == 4   # 4 distinct rows, no duplicates
        finally:
            project.run_sql("DROP TABLE {schema}.otf_inc_src")


# ===================================================================
# Scenario 16: OTF incremental append with PARTITIONED BY
# ===================================================================

otf_inc_append_partitioned_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='append',
    partitioned_by='YEAR(created_date)'
) }}}}
select id, name, created_date from {{{{ target.schema }}}}.otf_inc_src_dated

{{% if is_incremental() %}}
    where created_date > (select max(created_date) from {{{{ this }}}})
{{% endif %}}
"""


class TestOTFIncrementalAppendPartitioned(BaseCatalogIntegrationValidation):
    """Verify OTF incremental append with PARTITIONED BY.
    The first run creates a partitioned OTF table, second run appends.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "otf_inc_append_partitioned.sql": otf_inc_append_partitioned_sql,
        }

    def test_incremental_append_partitioned(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_inc_src_dated "
            "(id INTEGER, name VARCHAR(100), created_date DATE)"
        )
        project.run_sql(
            "INSERT INTO {schema}.otf_inc_src_dated "
            "VALUES (1, 'alice', DATE '2024-01-15')"
        )
        try:
            # First run: creates partitioned OTF table.
            results = run_dbt(["run", "--select", "otf_inc_append_partitioned"])
            assert len(results) == 1
            assert results[0].status == "success"

            # Verify first run produced exactly 1 row.
            count1 = project.run_sql(
                f'SELECT COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_inc_append_partitioned"',
                fetch="one",
            )[0]
            assert count1 == 1

            # Delete id=1 from the source to prove the second run is truly
            # append-only (existing OTF rows must survive source deletions).
            project.run_sql("DELETE FROM {schema}.otf_inc_src_dated WHERE id = 1")

            # Add a row in a new partition (later date → passes the incremental filter).
            project.run_sql(
                "INSERT INTO {schema}.otf_inc_src_dated "
                "VALUES (2, 'bob', DATE '2025-03-20')"
            )

            # Second run: should only append the new partition row.
            results = run_dbt(["run", "--select", "otf_inc_append_partitioned"])
            assert len(results) == 1
            assert results[0].status == "success"

            # Verify: alice (2024-01-15) still present + bob (2025-03-20) appended.
            count2 = project.run_sql(
                f'SELECT COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_inc_append_partitioned"',
                fetch="one",
            )[0]
            assert count2 == 2
        finally:
            project.run_sql("DROP TABLE {schema}.otf_inc_src_dated")


# ===================================================================
# Scenario 17: OTF incremental with --full-refresh
# ===================================================================

otf_inc_full_refresh_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='append'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_inc_src
"""


class TestOTFIncrementalFullRefresh(BaseCatalogIntegrationValidation):
    """Verify --full-refresh drops and recreates OTF incremental table.
    Covers: full refresh override on existing incremental OTF model.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_inc_full_refresh.sql": otf_inc_full_refresh_sql}

    def test_full_refresh_recreates_otf_table(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_inc_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_inc_src VALUES (1, 'alice')")
        try:
            # First run: creates.
            results = run_dbt(["run", "--select", "otf_inc_full_refresh"])
            assert len(results) == 1
            assert results[0].status == "success"

            # Full refresh: should DROP + CREATE from scratch.
            results = run_dbt([
                "run", "--select", "otf_inc_full_refresh", "--full-refresh"
            ])
            assert len(results) == 1
            assert results[0].status == "success"
        finally:
            project.run_sql("DROP TABLE {schema}.otf_inc_src")


# ===================================================================
# Scenario 18: dbt `alias` config on an OTF table materialization
#
# These exercise dbt's own `alias` resource config (NOT Teradata's
# CREATE ALIAS TABLE).  The expectation is that the physical OTF object is
# named after the *alias*, not the model file name, and that ref()/this
# resolve to the alias-named 3-part DATALAKE object.
# ===================================================================

# Model file is `otf_alias_model.sql`; the configured alias is a different name.
otf_alias_model_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    alias='otf_aliased_object'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_src
"""

# Downstream model refs by MODEL NAME (otf_alias_model); dbt must resolve that
# ref to the ALIAS-named OTF object in the compiled 3-part name.
downstream_of_alias_sql = """
{{ config(materialized='view') }}
select id, name from {{ ref('otf_alias_model') }}
"""


class TestOTFTableAlias(BaseCatalogIntegrationValidation):
    """dbt `alias` on an OTF `table` model: the OTF object is created under the
    alias, and the model-file name is NOT used as the object name.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "otf_alias_model.sql": otf_alias_model_sql,
            "downstream_of_alias.sql": downstream_of_alias_sql,
        }

    def test_alias_drives_otf_object_name(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_src VALUES (1, 'a')")
        project.run_sql("INSERT INTO {schema}.otf_src VALUES (2, 'b')")
        try:
            results = run_dbt(["run", "--select", "otf_alias_model"])
            assert len(results) == 1
            assert results[0].status == "success"

            # The OTF object must exist under the ALIAS name with 2 rows.
            count = project.run_sql(
                f'SELECT COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_aliased_object"',
                fetch="one",
            )[0]
            assert count == 2

            # The model-FILE name must NOT exist as an OTF object (Error 7825).
            with pytest.raises(Exception, match=r"\[Error (7825|6321)\]"):
                project.run_sql(
                    f'SELECT COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_model"',
                    fetch="one",
                )
        finally:
            project.run_sql("DROP TABLE {schema}.otf_src")

    def test_ref_to_aliased_otf_model_uses_alias_name(self, project):
        # ref('otf_alias_model') resolves by model name but must compile to the
        # ALIAS-named 3-part OTF object.
        run_dbt(["compile", "--select", "downstream_of_alias"])
        compiled_path = os.path.join(
            str(project.project_root),
            "target", "compiled", "test", "models", "downstream_of_alias.sql",
        )
        with open(compiled_path, "r", encoding="utf-8") as f:
            compiled = f.read()
        expected = f'"{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_aliased_object"'
        assert expected in compiled, (
            f"Expected alias-named 3-part OTF ref {expected!r} in compiled SQL, "
            f"got:\n{compiled}"
        )
        # The model-file name must not leak into the compiled relation name.
        assert f'"{OTF_DATABASE}"."otf_alias_model"' not in compiled


# ===================================================================
# Scenario 19: dbt `alias` config on an OTF incremental materialization
# ===================================================================

# Model file is `otf_alias_inc.sql`; alias renames the physical OTF object.
otf_alias_inc_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='append',
    alias='otf_alias_inc_object'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_inc_src

{{% if is_incremental() %}}
    where id > (select max(id) from {{{{ this }}}})
{{% endif %}}
"""


class TestOTFIncrementalAlias(BaseCatalogIntegrationValidation):
    """dbt `alias` on an OTF incremental model: create + append must both target
    the alias-named object, and `this` (used by the is_incremental filter and the
    existence probe) must resolve to the alias name.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_alias_inc.sql": otf_alias_inc_sql}

    def test_incremental_alias_create_then_append(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_inc_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_inc_src VALUES (1, 'alice')")
        project.run_sql("INSERT INTO {schema}.otf_inc_src VALUES (2, 'bob')")
        try:
            # First run: creates the alias-named OTF object.
            results = run_dbt(["run", "--select", "otf_alias_inc"])
            assert len(results) == 1
            assert results[0].status == "success"

            stats = project.run_sql(
                f'SELECT MIN(id), MAX(id), COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_inc_object"',
                fetch="one",
            )
            assert stats[0] == 1
            assert stats[1] == 2
            assert stats[2] == 2

            # The model-file name must NOT exist as an OTF object.
            with pytest.raises(Exception, match=r"\[Error (7825|6321)\]"):
                project.run_sql(
                    f'SELECT COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_inc"',
                    fetch="one",
                )

            # Delete id=1 from source to prove the second run is a true
            # incremental append, not a full rebuild.  If is_incremental() fires
            # correctly, the WHERE id > 2 filter excludes id=1 from the staging
            # payload, so id=1 (alice) must still be present in the OTF table
            # even though it no longer exists in the source.  A full rebuild
            # would produce OTF = [2, 3] (alice gone); a true append produces
            # OTF = [1, 2, 3] (alice preserved).
            project.run_sql("DELETE FROM {schema}.otf_inc_src WHERE id = 1")
            project.run_sql("INSERT INTO {schema}.otf_inc_src VALUES (3, 'charlie')")

            # Second run: append path must target the alias-named object via `this`.
            results = run_dbt(["run", "--select", "otf_alias_inc"])
            assert len(results) == 1
            assert results[0].status == "success"

            stats = project.run_sql(
                f'SELECT MIN(id), MAX(id), COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_inc_object"',
                fetch="one",
            )
            assert stats[0] == 1   # alice still present (append-only, not rebuilt)
            assert stats[1] == 3   # charlie is newest
            assert stats[2] == 3   # 3 rows: alice (run 1) + bob (run 1) + charlie (run 2)
        finally:
            project.run_sql("DROP TABLE {schema}.otf_inc_src")


# ===================================================================
# Scenario 20: dbt `alias` config + --full-refresh on OTF incremental
#
# Proves that --full-refresh drops and recreates the *alias-named* OTF
# object, not the model-file-named one.
# ===================================================================

otf_alias_fr_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='append',
    alias='otf_alias_fr_object'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_fr_src
"""


class TestOTFIncrementalAliasFullRefresh(BaseCatalogIntegrationValidation):
    """--full-refresh on an aliased OTF incremental model must DROP+CREATE the
    alias-named OTF object, not the model-file-named one.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_alias_fr_model.sql": otf_alias_fr_sql}

    def test_alias_full_refresh_recreates_alias_object(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_fr_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_fr_src VALUES (1, 'alice')")
        project.run_sql("INSERT INTO {schema}.otf_fr_src VALUES (2, 'bob')")
        try:
            # First run: creates alias-named OTF object with [alice, bob].
            results = run_dbt(["run", "--select", "otf_alias_fr_model"])
            assert len(results) == 1
            assert results[0].status == "success"

            count = project.run_sql(
                f'SELECT COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_fr_object"',
                fetch="one",
            )[0]
            assert count == 2

            # Mutate source: remove alice, add charlie.  An incremental run
            # would leave alice in OTF (append-only).  A full-refresh must
            # rebuild from the *current* source, so alice must disappear.
            project.run_sql("DELETE FROM {schema}.otf_fr_src WHERE id = 1")
            project.run_sql("INSERT INTO {schema}.otf_fr_src VALUES (3, 'charlie')")

            # --full-refresh: DROP + CREATE from current source [bob, charlie].
            results = run_dbt([
                "run", "--select", "otf_alias_fr_model", "--full-refresh"
            ])
            assert len(results) == 1
            assert results[0].status == "success"

            # OTF table must now contain exactly 2 rows: bob and charlie.
            # alice (id=1) must be gone — proves the table was rebuilt, not appended.
            stats = project.run_sql(
                f'SELECT MIN(id), MAX(id), COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_fr_object"',
                fetch="one",
            )
            assert stats[2] == 2   # exactly 2 rows after full-refresh
            assert stats[0] == 2   # bob is the minimum (alice gone)
            assert stats[1] == 3   # charlie is the maximum

            # The model-FILE name must NOT exist as an OTF object.
            with pytest.raises(Exception, match=r"\[Error (7825|6321)\]"):
                project.run_sql(
                    f'SELECT COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_fr_model"',
                    fetch="one",
                )
        finally:
            project.run_sql("DROP TABLE {schema}.otf_fr_src")


# ===================================================================
# Scenario 21: Long alias name (64 chars)
#
# Verifies that the identifier plumbing does not truncate or mangle a
# long alias.  The staging table name (<alias>__dbt_tmp = 73 chars) must
# also stay within Teradata's 128-char identifier limit.
# ===================================================================

otf_long_alias_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='append',
    alias='{OTF_LONG_ALIAS_NAME}'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_long_alias_src

{{% if is_incremental() %}}
    where id > (select max(id) from {{{{ this }}}})
{{% endif %}}
"""


class TestOTFLongAlias(BaseCatalogIntegrationValidation):
    """A 64-character alias must be written verbatim to the OTF catalog — the
    adapter must not truncate or mangle long-but-valid identifiers.  The
    staging table name (alias + '__dbt_tmp' = 73 chars) must also stay
    within Teradata's 128-char limit across both create and append runs.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_long_alias_model.sql": otf_long_alias_sql}

    def test_long_alias_create_and_append(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_long_alias_src (id INTEGER, name VARCHAR(100))"
        )
        project.run_sql("INSERT INTO {schema}.otf_long_alias_src VALUES (1, 'alice')")
        project.run_sql("INSERT INTO {schema}.otf_long_alias_src VALUES (2, 'bob')")
        try:
            # First run: creates the OTF table under the 64-char alias name.
            results = run_dbt(["run", "--select", "otf_long_alias_model"])
            assert len(results) == 1
            assert results[0].status == "success"

            count = project.run_sql(
                f'SELECT COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."{OTF_LONG_ALIAS_NAME}"',
                fetch="one",
            )[0]
            assert count == 2

            # The model-file name must NOT exist as an OTF object.
            with pytest.raises(Exception, match=r"\[Error (7825|6321)\]"):
                project.run_sql(
                    f'SELECT COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_long_alias_model"',
                    fetch="one",
                )

            # Delete id=1 to prove the incremental path fires correctly with the
            # long alias (staging table construction + existence probe both use it).
            project.run_sql("DELETE FROM {schema}.otf_long_alias_src WHERE id = 1")
            project.run_sql("INSERT INTO {schema}.otf_long_alias_src VALUES (3, 'charlie')")

            # Second run: append only id=3 via WHERE id > 2.
            results = run_dbt(["run", "--select", "otf_long_alias_model"])
            assert len(results) == 1
            assert results[0].status == "success"

            stats = project.run_sql(
                f'SELECT MIN(id), MAX(id), COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."{OTF_LONG_ALIAS_NAME}"',
                fetch="one",
            )
            assert stats[0] == 1   # alice preserved (append-only, not rebuilt)
            assert stats[1] == 3   # charlie appended
            assert stats[2] == 3
        finally:
            project.run_sql("DROP TABLE {schema}.otf_long_alias_src")


# ===================================================================
# Scenario 22: ref() to an aliased OTF incremental model
#
# Compile-only check: a downstream model that ref()s an aliased OTF
# incremental model must compile to the alias-named 3-part identifier,
# not the model file name.
# ===================================================================

otf_alias_inc_ref_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='append',
    alias='otf_alias_inc_ref_object'
) }}}}
select id, name from {{{{ target.schema }}}}.otf_inc_ref_src
{{% if is_incremental() %}}
    where id > (select max(id) from {{{{ this }}}})
{{% endif %}}
"""

downstream_of_alias_inc_sql = """
{{ config(materialized='view') }}
select id, name from {{ ref('otf_alias_inc_ref_model') }}
"""


class TestOTFRefToAliasedIncrementalModel(BaseCatalogIntegrationValidation):
    """ref() to an aliased OTF incremental model must compile to the
    alias-named 3-part DATALAKE object, not the model file name.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "otf_alias_inc_ref_model.sql": otf_alias_inc_ref_sql,
            "downstream_of_alias_inc.sql": downstream_of_alias_inc_sql,
        }

    def test_ref_to_aliased_incremental_uses_alias_name(self, project):
        run_dbt(["compile", "--select", "downstream_of_alias_inc"])
        compiled_path = os.path.join(
            str(project.project_root),
            "target", "compiled", "test", "models", "downstream_of_alias_inc.sql",
        )
        with open(compiled_path, "r", encoding="utf-8") as f:
            compiled = f.read()
        expected = f'"{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_inc_ref_object"'
        assert expected in compiled, (
            f"Expected alias-named 3-part OTF ref {expected!r} in compiled SQL, "
            f"got:\n{compiled}"
        )
        assert f'"{OTF_DATABASE}"."otf_alias_inc_ref_model"' not in compiled


# ===================================================================
# Scenario 23: dbt `alias` + `partitioned_by` on OTF incremental
#
# Verifies that alias and partitioned_by work together end-to-end:
# the alias-named OTF table is created with the partition spec, and
# the incremental append targets the alias-named object correctly.
# ===================================================================

otf_alias_partitioned_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='append',
    alias='otf_alias_partitioned_object',
    partitioned_by='YEAR(event_date)'
) }}}}
select id, name, event_date
from {{{{ target.schema }}}}.otf_alias_partitioned_src
{{% if is_incremental() %}}
    where event_date > (select max(event_date) from {{{{ this }}}})
{{% endif %}}
"""


class TestOTFAliasWithPartition(BaseCatalogIntegrationValidation):
    """alias + partitioned_by on OTF incremental: the alias-named table is
    created with the partition spec and the incremental append targets the
    correct alias-named 3-part object across two year-partitions.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_alias_partitioned_model.sql": otf_alias_partitioned_sql}

    def test_alias_partitioned_create_and_append(self, project):
        project.run_sql(
            "CREATE TABLE {schema}.otf_alias_partitioned_src "
            "(id INTEGER, name VARCHAR(100), event_date DATE)"
        )
        project.run_sql(
            "INSERT INTO {schema}.otf_alias_partitioned_src "
            "VALUES (1, 'alice', DATE '2024-01-15')"
        )
        project.run_sql(
            "INSERT INTO {schema}.otf_alias_partitioned_src "
            "VALUES (2, 'bob', DATE '2024-03-20')"
        )
        try:
            # First run: creates partitioned OTF table under the alias name.
            results = run_dbt(["run", "--select", "otf_alias_partitioned_model"])
            assert len(results) == 1
            assert results[0].status == "success"

            stats = project.run_sql(
                f'SELECT MIN(id), MAX(id), COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_partitioned_object"',
                fetch="one",
            )
            assert stats[2] == 2  # alice and bob

            # The model-file name must NOT exist as an OTF object.
            with pytest.raises(Exception, match=r"\[Error (7825|6321)\]"):
                project.run_sql(
                    f'SELECT COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_partitioned_model"',
                    fetch="one",
                )

            # Delete alice from source; add charlie in a new year-partition.
            # A true incremental run must preserve alice in the OTF table.
            project.run_sql(
                "DELETE FROM {schema}.otf_alias_partitioned_src WHERE id = 1"
            )
            project.run_sql(
                "INSERT INTO {schema}.otf_alias_partitioned_src "
                "VALUES (3, 'charlie', DATE '2025-06-10')"
            )

            # Second run: WHERE event_date > '2024-03-20' appends only charlie.
            results = run_dbt(["run", "--select", "otf_alias_partitioned_model"])
            assert len(results) == 1
            assert results[0].status == "success"

            # OTF table must have 3 rows: alice (preserved), bob, charlie.
            stats = project.run_sql(
                f'SELECT MIN(id), MAX(id), COUNT(*) FROM "{DATALAKE_NAME}"."{OTF_DATABASE}"."otf_alias_partitioned_object"',
                fetch="one",
            )
            assert stats[0] == 1   # alice preserved (append-only, not rebuilt)
            assert stats[1] == 3   # charlie appended
            assert stats[2] == 3   # 3 rows: alice + bob + charlie
        finally:
            project.run_sql("DROP TABLE {schema}.otf_alias_partitioned_src")
