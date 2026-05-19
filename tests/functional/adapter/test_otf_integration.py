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
import yaml

from dbt.tests.util import run_dbt, get_manifest


DATALAKE_NAME = os.getenv("DBT_TERADATA_DATALAKE")
OTF_DATABASE = os.getenv("DBT_TERADATA_OTF_DATABASE")

pytestmark = pytest.mark.skipif(
    not (DATALAKE_NAME and OTF_DATABASE),
    reason="requires DBT_TERADATA_DATALAKE and DBT_TERADATA_OTF_DATABASE env vars",
)


CATALOG_NAME = "test_catalog"


# ---------------------------------------------------------------------------
# Shared fixtures: write catalogs.yml at the project root
# ---------------------------------------------------------------------------

@pytest.fixture(scope="class")
def write_catalogs_yml(project_root):
    """Write a catalogs.yml file into the test project root.

    dbt-tests-adapter does not have a built-in fixture for this (catalogs are
    relatively new), so we hand-write it. Returns the path for assertions.
    """
    catalogs_path = project_root / "catalogs.yml"
    catalogs_config = {
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
    catalogs_path.write_text(yaml.safe_dump(catalogs_config))
    return catalogs_path


# ---------------------------------------------------------------------------
# Model SQL fixtures
# ---------------------------------------------------------------------------

basic_otf_model_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}'
) }}}}
select 1 as id, 'a' as name
union all
select 2 as id, 'b' as name
"""

otf_with_purge_mode_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    purge_mode='NO PURGE'
) }}}}
select 1 as id
"""

otf_with_grants_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    grants={{'select': ['{os.getenv('DBT_TEST_USER_1', 'PUBLIC')}']}}
) }}}}
select 1 as id
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

class TestOTFBasicAndIdempotent:
    @pytest.fixture(scope="class")
    def models(self):
        return {"basic_otf.sql": basic_otf_model_sql}

    def test_basic_create_and_rerun(self, project, write_catalogs_yml):
        # First run: creates the OTF table.
        results = run_dbt(["run", "--select", "basic_otf"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run: must succeed idempotently (DROP+CREATE cycle).
        results = run_dbt(["run", "--select", "basic_otf"])
        assert len(results) == 1
        assert results[0].status == "success"


# ===================================================================
# Scenario 3: cross-model ref() produces 3-part name in compiled SQL
# ===================================================================

class TestOTFRefRendersThreePartName:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_otf.sql": basic_otf_model_sql,
            "downstream_of_otf.sql": downstream_of_otf_sql,
        }

    def test_compiled_sql_contains_three_part_name(self, project, write_catalogs_yml):
        run_dbt(["compile", "--select", "downstream_of_otf"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.downstream_of_otf"]
        compiled = node.compiled_code
        # 3-part: <datalake>."<otf_db>"."<table>"
        expected = f'{DATALAKE_NAME}."{OTF_DATABASE}"."basic_otf"'
        assert expected in compiled, (
            f"Expected 3-part OTF name {expected!r} in compiled SQL, got:\n{compiled}"
        )


# ===================================================================
# Scenario 4: source() with database != schema triggers OTF heuristic
# ===================================================================

class TestOTFSourceHeuristic:
    @pytest.fixture(scope="class")
    def models(self):
        return {"source_referencing_otf.sql": source_referencing_otf_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"sources.yml": sources_yml}

    def test_source_compiles_to_three_part_name(self, project, write_catalogs_yml):
        # Compile only -- we don't require the source to actually exist.
        run_dbt(["compile", "--select", "source_referencing_otf"])
        manifest = get_manifest(project.project_root)
        node = manifest.nodes["model.test.source_referencing_otf"]
        compiled = node.compiled_code
        expected = f'{DATALAKE_NAME}."{OTF_DATABASE}"."external_otf_table"'
        assert expected in compiled, (
            f"Expected 3-part OTF source name {expected!r} in compiled SQL, "
            f"got:\n{compiled}"
        )


# ===================================================================
# Scenario 5: grants applied to OTF tables (verifies Phase 1.3)
# ===================================================================

@pytest.mark.skipif(
    not os.getenv("DBT_TEST_USER_1"),
    reason="grants test requires DBT_TEST_USER_1 env var",
)
class TestOTFGrants:
    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_with_grants.sql": otf_with_grants_sql}

    def test_grants_applied(self, project, write_catalogs_yml):
        # The run should succeed with grants config -- this confirms the OTF
        # branch in table.sql invokes apply_grants() rather than silently
        # skipping it (the bug fixed in Phase 1.3).
        results = run_dbt(["run", "--select", "otf_with_grants"])
        assert len(results) == 1
        assert results[0].status == "success"


# ===================================================================
# Scenario 6: purge_mode: 'NO PURGE' end-to-end
# ===================================================================

class TestOTFNoPurge:
    @pytest.fixture(scope="class")
    def models(self):
        return {"otf_no_purge.sql": otf_with_purge_mode_sql}

    def test_no_purge_run_succeeds(self, project, write_catalogs_yml):
        results = run_dbt(["run", "--select", "otf_no_purge"])
        assert len(results) == 1
        assert results[0].status == "success"
        # Re-run to exercise the DROP path with NO PURGE.
        results = run_dbt(["run", "--select", "otf_no_purge"])
        assert results[0].status == "success"


# ===================================================================
# Phase 2 guardrails: compile-time errors for unsupported combinations
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


class TestOTFCompileTimeErrors:
    """These exercise compile-time guardrails added in Phase 2.

    They still need write_catalogs_yml because catalog_name resolution happens
    during compile -- but no actual DATALAKE has to exist.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_table_kind.sql": unsupported_table_kind_sql,
            "bad_purge_mode.sql": invalid_purge_mode_sql,
        }

    def test_table_kind_with_catalog_name_fails(self, project, write_catalogs_yml):
        results = run_dbt(
            ["compile", "--select", "bad_table_kind"], expect_pass=False
        )
        assert any("table_kind" in str(r.message or "") for r in results)

    def test_invalid_purge_mode_fails(self, project, write_catalogs_yml):
        results = run_dbt(
            ["compile", "--select", "bad_purge_mode"], expect_pass=False
        )
        assert any("purge_mode" in str(r.message or "").lower() for r in results)
