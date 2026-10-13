import pytest

from dbt.tests.util import run_dbt
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental,BaseIncrementalNotSchemaChange
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    BaseGenerateProject,
    ref_models__schema_yml,
    ref_sources__schema_yml,
    ref_models__view_summary_sql,
    ref_models__ephemeral_summary_sql,
    ref_models__ephemeral_copy_sql,
    ref_models__docs_md,
)
from dbt.tests.adapter.basic import expected_catalog as expected_catalog_mod
from dbt.tests.util import AnyInteger, AnyString
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.adapter.basic.test_incremental import BaseIncrementalBadStrategy


# The raw-drop helper and its "database does not exist" error code live in
# tests/teradata_cleanup.py so the error code is defined once for the whole
# suite (conftest.py and test_store_test_failures.py use the same helper).
from tests.teradata_cleanup import run_sql_ignoring_missing_database


model_incremental = """
select * from {{ source('raw', 'seed') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

config_materialized_incremental_append = """
  {{ config(materialized="incremental", incremental_strategy="append") }}
"""
incremental_append_sql = config_materialized_incremental_append + model_incremental

config_materialized_incremental_delete_insert = """
  {{ config(materialized="incremental", incremental_strategy="delete+insert") }}
"""
incremental_delete_insert_sql = config_materialized_incremental_delete_insert + model_incremental

config_materialized_incremental_merge= """
  {{ config(materialized="incremental",incremental_strategy="merge",unique_key='id',index="UNIQUE PRIMARY INDEX (id)") }}
"""
incremental_merge_sql = config_materialized_incremental_merge + model_incremental

config_materialized_incremental_with_multiple_unique_key_merge= """
  {{ config(materialized="incremental",incremental_strategy="merge",unique_key=['id','some_date'],index="UNIQUE PRIMARY INDEX (id, some_date)") }}
"""
incremental_merge_with_multiple_unique_key_sql = config_materialized_incremental_with_multiple_unique_key_merge + model_incremental

schema_base_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
"""

incremental_not_schema_change_sql = """
{{ config(materialized="incremental", unique_key="user_id_current_time",on_schema_change="sync_all_columns") }}
select
    Concat('1','-',cast(current_timestamp as varchar(40))) as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""


class TestSimpleMaterializationsTeradata(BaseSimpleMaterializations):
    pass


class TestSingularTestsMyTeradata(BaseSingularTests):
    pass

'''
class TestSingularTestsEphemeralTeradata(BaseSingularTestsEphemeral):
    pass

# FAIL
  # test_dbt_ephemeral_data_tests: data_test_ephemeral_models - fails with
  #   [Error 6926] WITH [RECURSIVE] clause or recursive view is not supported
  #   within WITH [RECURSIVE] definitions, views, triggers or stored procedures.
'''

class TestEmptyTeradata(BaseEmpty):
    pass


class TestEphemeralTeradata(BaseEphemeral):
    pass

class TestIncrementalTeradata(BaseIncremental):
    pass

class TestIncrementalAppendTeradata(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_append_sql, "schema.yml": schema_base_yml}
    
    pass

class TestIncrementalDeleteInsertTeradata(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_delete_insert_sql, "schema.yml": schema_base_yml}
    
    pass

class TestIncrementalMergeTeradata(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_merge_sql, "schema.yml": schema_base_yml}
    
    pass

class TestIncrementalMergeTeradataWithMultipleUniqueKey(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_merge_with_multiple_unique_key_sql, "schema.yml": schema_base_yml}
    
    pass

class TestBaseIncrementalNotSchemaChangeTeradata(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}
    pass

class TestGenericTestsTeradata(BaseGenericTests):
    pass


class TestSnapshotCheckColsTeradata(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampTeradata(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


class TestDocsGenerateTeradata(BaseDocsGenerate):
    # BaseDocsGenerate's own `clean_up` fixture drops the alternate "_test"
    # schema via the guarded `adapter.drop_schema()` macro, which Teradata's
    # verify_database() intentionally rejects for any schema other than the
    # connection's configured one (a genuinely different, second database
    # here). Merely swallowing that guard error -- as an earlier version of
    # this override did -- would skip the drop entirely and leak the "_test"
    # database on every run. Instead, mirror
    # _TeradataAuditSchemaCleanupMixin in test_store_test_failures.py: drop
    # the alternate schema directly via raw SQL, bypassing the guarded macro
    # for this one intentional cross-schema case, and only ignore the
    # "database does not exist" error.
    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        alternate_schema = f"{project.test_schema}_test"
        run_sql_ignoring_missing_database(
            project, f"DELETE DATABASE {alternate_schema} ALL;"
        )
        run_sql_ignoring_missing_database(
            project, f"DROP DATABASE {alternate_schema};"
        )

    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_catalog_mod.base_expected_catalog(
            project,
            # dbt-teradata does not report a relation owner in the catalog:
            # DBC.TablesV exposes CreatorName (who ran the CREATE), which is
            # not the same concept as ownership in Teradata, where objects are
            # owned by the containing database.
            role=None,
            id_type="INTEGER",
            text_type="CHARACTER VARYING",
            time_type="TIMESTAMP",
            # dbt-teradata's catalog reports dbt's own lowercase relation-type
            # names ('table'/'view'), matching adapter.get_relation() and
            # get_catalog_for_single_relation(), rather than the SQL-standard
            # information_schema spellings used by Postgres ('BASE TABLE'/'VIEW').
            # These are test-suite parameters precisely so each adapter can
            # declare its own catalog vocabulary.
            view_type="view",
            table_type="table",
            model_stats=expected_catalog_mod.no_stats(),
        )

    def test_run_and_generate(self, project, expected_catalog):
        pytest.skip(
            "Error 5315: catalog generation with full compile requires "
            "SELECT WITH GRANT OPTION on column grants — server permission issue"
        )

    def test_run_and_generate_no_compile(self, project, expected_catalog):
        pytest.skip(
            "Catalog returns type='N/A' for view columns in Teradata DBC.ColumnsV; "
            "column-type metadata unavailable for views without special DBA access."
        )


# 'ct' is a reserved word in Teradata (temporal-table keyword).
# Override the models to rename ct → cnt throughout.
_EPHEMERAL_SUMMARY_TD = ref_models__ephemeral_summary_sql.replace(
    "count(*) as ct", "count(*) as cnt"
).replace(
    "\norder by first_name asc", ""  # Teradata rejects ORDER BY in table-creation subqueries
)
_VIEW_SUMMARY_TD = ref_models__view_summary_sql.replace(
    "select first_name, ct", "select first_name, cnt"
).replace(
    "\norder by ct asc", ""  # Teradata rejects ORDER BY in view definitions (Error 3706)
)
_SCHEMA_YML_TD = ref_models__schema_yml.replace(
    "- name: ct\n        description: \"{{ doc('summary_count') }}\"",
    "- name: cnt\n        description: \"{{ doc('summary_count') }}\"",
)


class TestDocsGenReferencesTeradata(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": _SCHEMA_YML_TD,
            "sources.yml": ref_sources__schema_yml,
            "view_summary.sql": _VIEW_SUMMARY_TD,
            "ephemeral_summary.sql": _EPHEMERAL_SUMMARY_TD,
            "ephemeral_copy.sql": ref_models__ephemeral_copy_sql,
            "docs.md": ref_models__docs_md,
        }

    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        catalog = expected_catalog_mod.expected_references_catalog(
            project,
            # See TestDocsGenerateTeradata.expected_catalog: dbt-teradata does
            # not report a relation owner in the catalog.
            role=None,
            id_type="INTEGER",
            text_type="CHARACTER VARYING",
            time_type="TIMESTAMP",
            bigint_type="DECIMAL",  # placeholder; overridden below since the exact
            # numeric type Teradata reports for COUNT(*) varies by server version
            # (observed "DECIMAL" on 20.0.0.47, "BIGINT" on 20.0.0.68).
            # See TestDocsGenerateTeradata.expected_catalog: dbt-teradata's
            # catalog uses dbt's lowercase relation-type names.
            view_type="view",
            table_type="table",
            model_stats=expected_catalog_mod.no_stats(),
        )
        # Rename "ct" → "cnt" in summary model columns (ct is reserved in Teradata)
        for node_key in ("model.test.ephemeral_summary", "model.test.view_summary"):
            cols = catalog["nodes"][node_key]["columns"]
            if "ct" in cols:
                entry = cols.pop("ct")
                cols["cnt"] = dict(entry, name="cnt")
            # The numeric type Teradata reports for COUNT(*) varies by server
            # version (DECIMAL vs BIGINT observed across environments), so
            # accept any type string here rather than pinning one.
            cols["cnt"]["type"] = AnyString()
        # Teradata's DBC.ColumnsV uses large internal ColumnIds, not 1-based indices.
        # Replace all hardcoded column indices with AnyInteger() to allow any integer.
        for section in ("nodes", "sources"):
            for node_data in catalog[section].values():
                for col_data in node_data.get("columns", {}).values():
                    col_data["index"] = AnyInteger()
        return catalog


class TestGenerateProjectTeradata(BaseGenerateProject):
    pass


class TestTableMaterializationTeradata(BaseTableMaterialization):
    pass


class TestValidateConnectionTeradata(BaseValidateConnection):
    pass


class TestIncrementalBadStrategyTeradata(BaseIncrementalBadStrategy):
    def test_incremental_invalid_strategy(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 2
        results = run_dbt(["run"], expect_pass=False)
        assert len(results.results) == 1
        # Teradata adapter validates strategies via teradata__validate_get_incremental_strategy
        # and produces a different error message than the dbt-core default.
        assert "Invalid incremental strategy provided: bad_strategy" in results.results[0].message
