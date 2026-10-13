"""
Functional tests for merge_update_columns / merge_exclude_columns in dbt-teradata.

Design notes
------------
* Three-column seeds only (id, status, amount) — no varchar name column to avoid
  CHAR-width mismatches between seed_merge (VARCHAR(7) for "Charlie") and
  seed_update (VARCHAR(5) for "Alice") after MERGE UPDATE.
* depends_on hint required because ref() is inside {% if is_incremental() %}.
* Two-run pattern: run 1 creates the table (no MERGE), run 2 exercises the MERGE path.
* "got N difference" in check_relations_equal means symmetric diff of rows.
"""

import pytest
from dbt.tests.util import check_relations_equal, run_dbt

# ---------------------------------------------------------------------------
# Seeds
# ---------------------------------------------------------------------------

SEED_MERGE_CSV = """id,status,amount
1,active,100
2,active,200
3,active,300"""

# Incremental source: updated values for id=1 and id=2 only
SEED_UPDATE_CSV = """id,status,amount
1,inactive,999
2,inactive,888"""

SEEDS = {
    "seed_merge.csv": SEED_MERGE_CSV,
    "seed_update.csv": SEED_UPDATE_CSV,
}


def _model(config_str: str) -> str:
    return (
        f"{config_str}\n"
        "-- depends_on: {{ ref('seed_update') }}\n"
        "\n"
        "{% if is_incremental() %}\n"
        "select * from {{ ref('seed_update') }}\n"
        "{% else %}\n"
        "select * from {{ ref('seed_merge') }}\n"
        "{% endif %}\n"
    )


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

MODEL_UPDATE_SQL = _model(
    "{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id', merge_update_columns=['status']) }}"
)

MODEL_EXCLUDE_SQL = _model(
    "{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id', merge_exclude_columns=['amount']) }}"
)

MODEL_KEY_IN_UPDATE_SQL = _model(
    "{{ config(materialized='incremental', incremental_strategy='merge', unique_key='ID', merge_update_columns=['id','status']) }}"
)

MODEL_ONLY_KEY_SQL = _model(
    "{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id', merge_update_columns=['id']) }}"
)

MODEL_EXCLUDE_ALL_SQL = _model(
    "{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id', merge_exclude_columns=['status','amount']) }}"
)

MODEL_DEFAULT_SQL = _model(
    "{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id') }}"
)

MODEL_BOTH_CONFIGS_SQL = _model(
    "{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id', merge_update_columns=['status'], merge_exclude_columns=['amount']) }}"
)

# Destination genuinely has only the unique_key column (no merge_update_columns/
# merge_exclude_columns configured) -> WHEN MATCHED must be silently skipped, not
# raise (see teradata__get_merge_sql "key-only model" branch).
MODEL_KEY_ONLY_NO_OPTIONS_SQL = (
    "{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id') }}\n"
    "-- depends_on: {{ ref('seed_update') }}\n"
    "\n"
    "{% if is_incremental() %}\n"
    "select id from {{ ref('seed_update') }}\n"
    "{% else %}\n"
    "select id from {{ ref('seed_merge') }}\n"
    "{% endif %}\n"
)

# ---------------------------------------------------------------------------
# Expected-table SQL
# (all reference seeds so Teradata always has a FROM clause)
# ---------------------------------------------------------------------------

# merge_update_columns=['status']: only status updated; amount preserved from target
EXPECTED_STATUS_ONLY_SQL = """\
{{ config(materialized='table') }}
select
    m.id,
    coalesce(u.status, m.status) as status,
    m.amount
from {{ ref('seed_merge') }} m
left join {{ ref('seed_update') }} u on m.id = u.id
"""

# merge_exclude_columns=['amount']: status updated, amount preserved
EXPECTED_EXCLUDE_AMOUNT_SQL = """\
{{ config(materialized='table') }}
select
    m.id,
    coalesce(u.status, m.status) as status,
    m.amount
from {{ ref('seed_merge') }} m
left join {{ ref('seed_update') }} u on m.id = u.id
"""

# default: all columns updated
EXPECTED_ALL_UPDATED_SQL = """\
{{ config(materialized='table') }}
select
    m.id,
    coalesce(u.status, m.status) as status,
    coalesce(u.amount, m.amount) as amount
from {{ ref('seed_merge') }} m
left join {{ ref('seed_update') }} u on m.id = u.id
"""

# key-only model, no column options: WHEN MATCHED is skipped, so target ids are
# unchanged (id=3 stays; ids 1,2 stay matched/untouched; no new ids to insert).
EXPECTED_KEY_ONLY_SQL = """\
{{ config(materialized='table') }}
select id from {{ ref('seed_merge') }}
"""

# ===========================================================================
# Test classes
# ===========================================================================

class TestMergeUpdateColumns:
    @pytest.fixture(scope="class")
    def seeds(self):
        return SEEDS

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns.sql": MODEL_UPDATE_SQL,
            "merge_update_columns_expected.sql": EXPECTED_STATUS_ONLY_SQL,
        }

    def test_merge_update_columns(self, project):
        """merge_update_columns=['status'] updates only status; amount preserved."""
        run_dbt(["seed"])
        run_dbt(["run", "--select", "merge_update_columns"])
        run_dbt(["run", "--select", "merge_update_columns"])
        run_dbt(["run", "--select", "merge_update_columns_expected"])
        check_relations_equal(
            project.adapter,
            ["merge_update_columns", "merge_update_columns_expected"],
        )


class TestMergeExcludeColumns:
    @pytest.fixture(scope="class")
    def seeds(self):
        return SEEDS

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_exclude_columns.sql": MODEL_EXCLUDE_SQL,
            "merge_exclude_columns_expected.sql": EXPECTED_EXCLUDE_AMOUNT_SQL,
        }

    def test_merge_exclude_columns(self, project):
        """merge_exclude_columns=['amount'] updates status but preserves amount."""
        run_dbt(["seed"])
        run_dbt(["run", "--select", "merge_exclude_columns"])
        run_dbt(["run", "--select", "merge_exclude_columns"])
        run_dbt(["run", "--select", "merge_exclude_columns_expected"])
        check_relations_equal(
            project.adapter,
            ["merge_exclude_columns", "merge_exclude_columns_expected"],
        )


class TestMergeKeyInUpdateColumns:
    @pytest.fixture(scope="class")
    def seeds(self):
        return SEEDS

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_key_in_update.sql": MODEL_KEY_IN_UPDATE_SQL,
            "merge_key_in_update_expected.sql": EXPECTED_STATUS_ONLY_SQL,
        }

    def test_key_in_update_columns_does_not_raise(self, project):
        """unique_key='ID' (different case) inside merge_update_columns=['id',...] must be
        case-insensitively dropped, not raise Error 5977."""
        run_dbt(["seed"])
        run_dbt(["run", "--select", "merge_key_in_update"])
        run_dbt(["run", "--select", "merge_key_in_update"])
        run_dbt(["run", "--select", "merge_key_in_update_expected"])
        check_relations_equal(
            project.adapter,
            ["merge_key_in_update", "merge_key_in_update_expected"],
        )


class TestMergeOnlyKeyInUpdateColumns:
    @pytest.fixture(scope="class")
    def seeds(self):
        return SEEDS

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_only_key.sql": MODEL_ONLY_KEY_SQL,
        }

    def test_only_key_raises_compiler_error(self, project):
        """merge_update_columns=['id'] leaves no updatable columns -> compiler error on the incremental run."""
        run_dbt(["seed"])
        # First run: full-refresh (table absent) -> CTAS, MERGE macro not called -> no error
        run_dbt(["run", "--select", "merge_only_key"], expect_pass=True)
        # Second run: incremental path -> teradata__get_merge_sql raises CompilerError (empty SET)
        result = run_dbt(["run", "--select", "merge_only_key"], expect_pass=False).results[0]
        assert "leaves no updatable columns" in str(result.message)


class TestMergeExcludeAllColumns:
    @pytest.fixture(scope="class")
    def seeds(self):
        return SEEDS

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_exclude_all.sql": MODEL_EXCLUDE_ALL_SQL,
        }

    def test_exclude_all_raises_compiler_error(self, project):
        """merge_exclude_columns covering every non-key column -> compiler error on the incremental run."""
        run_dbt(["seed"])
        # First run: full-refresh (table absent) -> CTAS, MERGE macro not called -> no error
        run_dbt(["run", "--select", "merge_exclude_all"], expect_pass=True)
        # Second run: incremental path -> teradata__get_merge_sql raises CompilerError (empty SET)
        result = run_dbt(["run", "--select", "merge_exclude_all"], expect_pass=False).results[0]
        assert "leaves no updatable columns" in str(result.message)


class TestMergeKeyOnlyNoOptions:
    """Destination genuinely has only the unique_key column (no merge_update_columns/
    merge_exclude_columns configured). This must NOT raise -- WHEN MATCHED is silently
    skipped and the incremental MERGE degrades to insert-only, since there is nothing
    to update. Regression guard for Teradata Error 3707 (bare 'update set')."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return SEEDS

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_key_only.sql": MODEL_KEY_ONLY_NO_OPTIONS_SQL,
            "merge_key_only_expected.sql": EXPECTED_KEY_ONLY_SQL,
        }

    def test_key_only_model_succeeds_insert_only(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "merge_key_only"], expect_pass=True)
        run_dbt(["run", "--select", "merge_key_only"], expect_pass=True)
        run_dbt(["run", "--select", "merge_key_only_expected"])
        check_relations_equal(
            project.adapter,
            ["merge_key_only", "merge_key_only_expected"],
        )


class TestMergeDefault:
    @pytest.fixture(scope="class")
    def seeds(self):
        return SEEDS

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_default.sql": MODEL_DEFAULT_SQL,
            "merge_default_expected.sql": EXPECTED_ALL_UPDATED_SQL,
        }

    def test_default_merge_unchanged(self, project):
        """No column options -> full-row update (regression guard)."""
        run_dbt(["seed"])
        run_dbt(["run", "--select", "merge_default"])
        run_dbt(["run", "--select", "merge_default"])
        run_dbt(["run", "--select", "merge_default_expected"])
        check_relations_equal(
            project.adapter,
            ["merge_default", "merge_default_expected"],
        )


class TestMergeBothColumnConfigs:
    @pytest.fixture(scope="class")
    def seeds(self):
        return SEEDS

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_both_configs.sql": MODEL_BOTH_CONFIGS_SQL,
        }

    def test_both_configs_raises_error(self, project):
        """Setting both merge_update_columns and merge_exclude_columns must raise a compile error."""
        run_dbt(["seed"])
        # First run: full-refresh (table absent) -> CTAS, MERGE macro not called -> no error
        run_dbt(["run", "--select", "merge_both_configs"], expect_pass=True)
        # Second run: incremental path -> teradata__get_merge_sql raises CompilerError
        result = run_dbt(["run", "--select", "merge_both_configs"], expect_pass=False).results[0]
        assert "cannot specify" in str(result.message).lower()
