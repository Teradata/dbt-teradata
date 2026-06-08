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
"""

import os

import pytest

from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt


DATALAKE_NAME = os.getenv("DBT_TERADATA_DATALAKE")
OTF_DATABASE = os.getenv("DBT_TERADATA_OTF_DATABASE")

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
]


@pytest.fixture(autouse=True)
def _cleanup_otf_tables(project):
    yield
    for name in _OTF_TEST_TABLES:
        project.run_sql(
            f'DROP TABLE /*+ IF EXISTS */ "{DATALAKE_NAME}"."{OTF_DATABASE}"."{name}" NO PURGE;'
        )


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
        a model with write.format.default='orc' must fail (not succeed)."""
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

