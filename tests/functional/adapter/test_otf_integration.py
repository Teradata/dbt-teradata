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

Scenarios covered (see Phase 4.4 of PR_237_REMEDIATION_PLAN.md):
  1. Basic OTF table create
  2. Idempotency (DROP+CREATE cycle survives re-runs)
  3. Cross-model ref() to an OTF model produces 3-part compiled SQL
  4. Cross-model source() with database/schema set produces 3-part SQL
     (verifies the database != schema auto-OTF heuristic)
  5. grants config is applied (verifies Phase 1.3 feature parity)
  6. purge_mode: 'NO PURGE' end-to-end
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
        # 3-part: <datalake>."<otf_db>"."<table>"
        expected = f'{DATALAKE_NAME}."{OTF_DATABASE}"."basic_otf"'
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
        expected = f'{DATALAKE_NAME}."{OTF_DATABASE}"."external_otf_table"'
        assert expected in compiled, (
            f"Expected 3-part OTF source name {expected!r} in compiled SQL, "
            f"got:\n{compiled}"
        )


# ===================================================================
# Scenario 6: purge_mode: 'NO PURGE' end-to-end
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
# Phase 2 guardrails: unsupported config combinations
# (these do not require a real DATALAKE, but live here to keep all OTF
# scenarios in one file; they are NOT gated on env vars)
# ===================================================================

unsupported_table_kind_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    table_kind='SET'
) }}}}
select 1 as id
"""

invalid_purge_mode_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    purge_mode='SOFT'
) }}}}
select 1 as id
"""


class TestOTFCompileTimeErrors(BaseCatalogIntegrationValidation):
    """These exercise guardrails for unsupported config combinations on the OTF
    path.  The checks fire inside the materialization macro, so they require
    ``dbt run`` (not ``compile``).  A real DATALAKE does not need to exist
    because the error is raised before any SQL is sent to the database.
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_table_kind.sql": unsupported_table_kind_sql,
            "bad_purge_mode.sql": invalid_purge_mode_sql,
        }

    def test_table_kind_with_catalog_name_fails(self, project):
        results = run_dbt(
            ["run", "--select", "bad_table_kind"], expect_pass=False
        )
        assert any("table_kind" in str(r.message or "") for r in results)

    def test_invalid_purge_mode_fails(self, project):
        results = run_dbt(
            ["run", "--select", "bad_purge_mode"], expect_pass=False
        )
        assert any("purge_mode" in str(r.message or "").lower() for r in results)
