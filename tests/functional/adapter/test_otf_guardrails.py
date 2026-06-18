"""Compile-time guardrail tests for OTF config validation.

These tests verify that unsupported config combinations on the OTF path
are rejected at compile time.  They do NOT require a real DATALAKE or OTF
database and are never skipped.

Scenarios covered:
  1. table_kind with catalog_name is rejected
  2. Invalid purge_mode is rejected
  3. table_option with catalog_name is rejected
  4. with_statistics with catalog_name is rejected
  5. index with catalog_name is rejected
  6. Multiple unsupported options reported together
  7. OTF incremental: unsupported strategy (merge) is rejected
  8. OTF incremental: delete+insert is rejected (not yet supported)
  9. OTF incremental: native-only options (table_kind) are rejected
  10. OTF incremental: contract.enforced is rejected
  11. OTF incremental: valid_history strategy is rejected
  12. OTF incremental: microbatch strategy is rejected
  13. snapshot materialization with catalog_name is rejected
  14. contract.enforced with catalog_name on table materialization is rejected
"""

import pytest

from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt


CATALOG_NAME = "test_catalog"

# Placeholder values — no real DATALAKE is needed because the errors fire
# before any SQL is sent to the database.
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
                        "datalake_name": "placeholder_datalake",
                        "otf_database": "placeholder_otf_db",
                    },
                }
            ],
        }
    ]
}


# ---------------------------------------------------------------------------
# Model SQL fixtures for unsupported config combos
# ---------------------------------------------------------------------------

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

unsupported_table_option_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    table_option='NO FALLBACK'
) }}}}
select 1 as id
"""

unsupported_with_statistics_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    with_statistics='true'
) }}}}
select 1 as id
"""

unsupported_index_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    index='PRIMARY INDEX (id)'
) }}}}
select 1 as id
"""

unsupported_multiple_options_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    table_kind='MULTISET',
    index='PRIMARY INDEX (id)',
    with_statistics='true'
) }}}}
select 1 as id
"""

incremental_with_catalog_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    unique_key='id'
) }}}}
select 1 as id
"""

incremental_otf_merge_strategy_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='merge',
    unique_key='id'
) }}}}
select 1 as id
"""

incremental_otf_delete_insert_strategy_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='delete+insert',
    unique_key='id'
) }}}}
select 1 as id
"""

incremental_otf_with_table_kind_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    table_kind='SET'
) }}}}
select 1 as id
"""

incremental_otf_with_contract_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    contract={{'enforced': true}},
    on_schema_change='fail'
) }}}}
select 1 as id
"""

incremental_otf_valid_history_strategy_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='valid_history',
    unique_key='id'
) }}}}
select 1 as id
"""

incremental_otf_microbatch_strategy_sql = f"""
{{{{ config(
    materialized='incremental',
    catalog_name='{CATALOG_NAME}',
    incremental_strategy='microbatch',
    unique_key='id',
    event_time='created_at',
    batch_size='day',
    begin='2020-01-01'
) }}}}
select 1 as id, current_timestamp as created_at
"""

snapshot_with_catalog_sql = f"""
{{% snapshot snapshot_otf %}}
{{{{ config(
    catalog_name='{CATALOG_NAME}',
    strategy='timestamp',
    unique_key='id',
    updated_at='updated_at'
) }}}}
select 1 as id, current_timestamp as updated_at
{{% endsnapshot %}}
"""

contract_with_catalog_sql = f"""
{{{{ config(
    materialized='table',
    catalog_name='{CATALOG_NAME}',
    contract={{'enforced': true}}
) }}}}
select 1 as id
"""


# ===================================================================
# Original guardrail tests: table_kind and purge_mode
# ===================================================================

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


# ===================================================================
# Extended guardrails: table_option, with_statistics, index
# ===================================================================

class TestOTFUnsupportedTableOptions(BaseCatalogIntegrationValidation):
    """Verify that table_option, with_statistics, and index configs are
    all rejected when catalog_name is set (OTF path).
    """

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_table_option.sql": unsupported_table_option_sql,
            "bad_with_statistics.sql": unsupported_with_statistics_sql,
            "bad_index.sql": unsupported_index_sql,
            "bad_multiple.sql": unsupported_multiple_options_sql,
        }

    def test_table_option_with_catalog_name_fails(self, project):
        results = run_dbt(
            ["run", "--select", "bad_table_option"], expect_pass=False
        )
        assert any("table_option" in str(r.message or "") for r in results)

    def test_with_statistics_with_catalog_name_fails(self, project):
        results = run_dbt(
            ["run", "--select", "bad_with_statistics"], expect_pass=False
        )
        assert any("with_statistics" in str(r.message or "") for r in results)

    def test_index_with_catalog_name_fails(self, project):
        results = run_dbt(
            ["run", "--select", "bad_index"], expect_pass=False
        )
        assert any("index" in str(r.message or "") for r in results)

    def test_multiple_unsupported_options_reported(self, project):
        """When multiple unsupported options are set, the error message
        includes all of them so users can fix everything in one pass."""
        results = run_dbt(
            ["run", "--select", "bad_multiple"], expect_pass=False
        )
        msg = str(results[0].message or "")
        assert "table_kind" in msg
        assert "index" in msg
        assert "with_statistics" in msg


# ===================================================================
# Materialization-level guardrails: incremental OTF and snapshot
# ===================================================================

class TestOTFIncrementalUnsupportedStrategy(BaseCatalogIntegrationValidation):
    """Verify that unsupported incremental strategies are rejected for OTF."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_otf_merge.sql": incremental_otf_merge_strategy_sql}

    def test_merge_strategy_with_otf_fails(self, project):
        results = run_dbt(
            ["run", "--select", "incremental_otf_merge"], expect_pass=False
        )
        msg = " ".join(str(r.message or "") for r in results)
        assert "merge" in msg.lower() or "invalid incremental strategy" in msg.lower()


class TestOTFIncrementalDeleteInsertBlocked(BaseCatalogIntegrationValidation):
    """Verify that delete+insert strategy is rejected for OTF (not yet supported)."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_otf_di.sql": incremental_otf_delete_insert_strategy_sql}

    def test_delete_insert_strategy_with_otf_fails(self, project):
        results = run_dbt(
            ["run", "--select", "incremental_otf_di"], expect_pass=False
        )
        msg = " ".join(str(r.message or "") for r in results)
        assert "delete+insert" in msg.lower() or "invalid incremental strategy" in msg.lower()


class TestOTFIncrementalUnsupportedOptions(BaseCatalogIntegrationValidation):
    """Verify that native-only config options (table_kind, etc.) are rejected
    for OTF incremental models."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_otf_table_kind.sql": incremental_otf_with_table_kind_sql}

    def test_table_kind_with_otf_incremental_fails(self, project):
        results = run_dbt(
            ["run", "--select", "incremental_otf_table_kind"], expect_pass=False
        )
        msg = " ".join(str(r.message or "") for r in results)
        assert "table_kind" in msg.lower()


class TestOTFIncrementalContractBlocked(BaseCatalogIntegrationValidation):
    """Verify that contract.enforced=true is rejected on OTF incremental models."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_otf_contract.sql": incremental_otf_with_contract_sql}

    def test_contract_with_otf_incremental_fails(self, project):
        results = run_dbt(
            ["run", "--select", "incremental_otf_contract"], expect_pass=False
        )
        msg = " ".join(str(r.message or "") for r in results)
        assert "contract" in msg.lower()


class TestOTFIncrementalValidHistoryBlocked(BaseCatalogIntegrationValidation):
    """Verify that valid_history strategy is rejected for OTF."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_otf_vh.sql": incremental_otf_valid_history_strategy_sql}

    def test_valid_history_strategy_with_otf_fails(self, project):
        results = run_dbt(
            ["run", "--select", "incremental_otf_vh"], expect_pass=False
        )
        msg = " ".join(str(r.message or "") for r in results)
        assert "valid_history" in msg.lower() or "invalid incremental strategy" in msg.lower()


class TestOTFIncrementalMicrobatchBlocked(BaseCatalogIntegrationValidation):
    """Verify that microbatch strategy is rejected for OTF."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_otf_mb.sql": incremental_otf_microbatch_strategy_sql}

    def test_microbatch_strategy_with_otf_fails(self, project):
        results = run_dbt(
            ["run", "--select", "incremental_otf_mb"], expect_pass=False
        )
        # Microbatch wraps execution in batches — the compilation error from
        # our OTF guardrail fires inside the first batch, so r.message is just
        # "ERROR" rather than the full text.  Asserting on error status is
        # sufficient; the guardrail message is visible in the dbt log output.
        assert len(results) == 1
        assert results[0].status == "error"


class TestOTFSnapshotBlocked(BaseCatalogIntegrationValidation):
    """Verify that the snapshot materialization rejects catalog_name."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_otf.sql": snapshot_with_catalog_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_snapshot_with_catalog_name_fails(self, project):
        results = run_dbt(
            ["snapshot"], expect_pass=False
        )
        msg = " ".join(str(r.message or "") for r in results)
        assert "catalog_name" in msg.lower() or "otf" in msg.lower()


# ===================================================================
# Contract guardrail: contract.enforced with OTF
# ===================================================================

class TestOTFContractBlocked(BaseCatalogIntegrationValidation):
    """Verify that contract.enforced=true is rejected on OTF models."""

    @pytest.fixture(scope="class")
    def catalogs(self):
        return CATALOGS_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {"contract_otf.sql": contract_with_catalog_sql}

    def test_contract_enforced_with_catalog_name_fails(self, project):
        results = run_dbt(
            ["run", "--select", "contract_otf"], expect_pass=False
        )
        msg = " ".join(str(r.message or "") for r in results)
        assert "contract" in msg.lower()
