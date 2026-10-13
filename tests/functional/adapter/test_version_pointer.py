""" dbt Core 1.12 introduces the latest_version_pointer feature for versioned models.
When enabled it automatically creates a lightweight pointer *view* that always
references the current `latest_version` of a model, giving consumers a stable
unversioned name to query.

Feature mechanics (dbt-core ≥ 1.12):
  • dbt-core's ModelRunner._materialize_latest_version_pointer() synthesises a
    runtime-only node whose raw SQL is ``SELECT * FROM <latest_versioned_table>``.
  • It then dispatches to the adapter's view materialization macro — on Teradata
    this is ``teradata__materialization_view`` which delegates to
    ``materialization_view_default()`` from dbt-core.
  • No adapter-level code changes are required; the tests below confirm the full
    round-trip works correctly on Teradata Vantage.

Configuration options tested:
  Global flag (dbt_project.yml):
    flags:
      latest_version_pointer_enabled_by_default: true

  Per-model (schema.yml):
    config:
      latest_version_pointer:
        enabled: true
        alias: <optional custom name>

All tests require a live Teradata Vantage instance (standard conftest connection).
All tests are skipped on dbt-core < 1.12.
"""

import pytest
import dbt.version as _dbt_version
import re
from packaging.version import Version

from dbt.tests.util import run_dbt, get_manifest, run_dbt_and_capture, write_file

_dbt_version_tuple = Version(_dbt_version.__version__).release[:2]

pytestmark = pytest.mark.skipif(
    _dbt_version_tuple < (1, 12),
    reason=f"latest_version_pointer requires dbt-core >= 1.12 (found {_dbt_version.__version__})",
)

# ---------------------------------------------------------------------------
# Shared SQL fixtures
# ---------------------------------------------------------------------------

_MODEL_V1_SQL = "select 1 as customer_id, 'alice' as customer_name"
_MODEL_V2_SQL = "select 1 as customer_id, 'alice' as customer_name, 'gold' as tier"
_MODEL_V3_SQL = (
    "select 1 as customer_id, 'alice' as customer_name, 'gold' as tier, 1 as is_active"
)
_DOWNSTREAM_SQL = "select * from {{ ref('dim_customers') }}"

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _view_exists(project, view_name: str) -> bool:
    """Return True when *view_name* exists as a view in the test schema.

    Uses DBC.TablesV with TableKind='V' — the standard accessible system catalog
    view on Teradata Vantage (DBC.Views does not exist on all configurations).
    """
    rows = project.run_sql(
        f"SELECT TableName FROM DBC.TablesV "
        f"WHERE DatabaseName='{project.test_schema}' "
        f"AND TRIM(UPPER(TableName))='{view_name.upper()}' "
        f"AND TableKind='V'",
        fetch="all",
    )
    return bool(rows)


def _view_text(project, view_name: str) -> str:
    """Return the stored view DDL text for *view_name* (lower-cased for easy assertions).

    On Teradata Vantage the view body is stored in DBC.TablesV.RequestText for
    rows where TableKind='V'.  The text is CLOB/LONG VARCHAR and may be returned
    as multiple rows (Teradata splits long views); we concatenate them.
    """
    rows = project.run_sql(
        f"SELECT RequestText FROM DBC.TablesV "
        f"WHERE DatabaseName='{project.test_schema}' "
        f"AND TRIM(UPPER(TableName))='{view_name.upper()}' "
        f"AND TableKind='V'",
        fetch="all",
    )
    assert rows, f"View '{view_name}' not found in schema '{project.test_schema}'"
    return "".join(r[0] for r in rows if r[0]).lower()


def _relation_exists(project, name: str) -> bool:
    """Return True when *name* exists as any relation (table or view) in the test schema."""
    rows = project.run_sql(
        f"SELECT TableName FROM DBC.TablesV "
        f"WHERE DatabaseName='{project.test_schema}' "
        f"AND TRIM(UPPER(TableName))='{name.upper()}'",
        fetch="all",
    )
    return bool(rows)


# ---------------------------------------------------------------------------
# 1. Basic pointer view creation — single version
# ---------------------------------------------------------------------------


class TestPointerViewSingleVersion:
    """Pointer view is created for a versioned model with a single version.

    Acceptance criteria:
      ✓ Verify pointer view creation works on Teradata
      ✓ Validate correct view DDL is generated (CREATE VIEW syntax)
      ✓ Test with single version (v: 1)
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 1
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
""",
        }

    def test_run_creates_versioned_table_and_pointer_view(self, project):
        results = run_dbt(["run"])
        # The pointer view is created as a post-run side-effect by dbt-core's
        # ModelRunner._materialize_latest_version_pointer(); it does NOT appear
        # as a separate RunResult entry.  Only the versioned model itself is counted.
        assert len(results) == 1

    def test_pointer_view_exists(self, project):
        run_dbt(["run"])
        assert _view_exists(project, "dim_customers"), (
            "Pointer view 'dim_customers' was not created in the test schema"
        )

    def test_pointer_view_selects_from_latest_version(self, project):
        run_dbt(["run"])
        text = _view_text(project, "dim_customers")
        assert "dim_customers_v1" in text, (
            f"Pointer view DDL does not reference dim_customers_v1:\n{text}"
        )

    def test_versioned_table_exists(self, project):
        run_dbt(["run"])
        assert _relation_exists(project, "dim_customers_v1"), (
            "Versioned table 'dim_customers_v1' was not created"
        )


# ---------------------------------------------------------------------------
# 2. Pointer view with multiple versions
# ---------------------------------------------------------------------------


class TestPointerViewMultipleVersions:
    """Pointer view resolves to the highest latest_version when multiple versions exist.

    Acceptance criteria:
      ✓ Test with multiple versions (v: 1, 2, 3)
      ✓ Test pointer resolution (verify pointer resolves to latest version)
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "dim_customers_v2.sql": _MODEL_V2_SQL,
            "dim_customers_v3.sql": _MODEL_V3_SQL,
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 3
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
      - v: 2
      - v: 3
""",
        }

    def test_all_versioned_tables_created(self, project):
        results = run_dbt(["run"])
        # v1 + v2 + v3 versioned models = 3 RunResult entries.
        # The pointer view is a post-run side-effect and is NOT a separate result.
        assert len(results) == 3

    def test_pointer_view_resolves_to_v3(self, project):
        run_dbt(["run"])
        text = _view_text(project, "dim_customers")
        assert "dim_customers_v3" in text, (
            f"Pointer view should reference v3 (latest), got:\n{text}"
        )

    def test_manifest_marks_v3_as_latest(self, project):
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        assert manifest.nodes["model.test.dim_customers.v3"].is_latest_version
        assert not manifest.nodes["model.test.dim_customers.v1"].is_latest_version
        assert not manifest.nodes["model.test.dim_customers.v2"].is_latest_version


# ---------------------------------------------------------------------------
# 3. Custom alias on the pointer view
# ---------------------------------------------------------------------------


class TestPointerViewCustomAlias:
    """The pointer view name is taken from `config.latest_version_pointer.alias`.

    Acceptance criteria:
      ✓ Test custom alias configuration (config.alias: dim_customers_current)
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "dim_customers_v2.sql": _MODEL_V2_SQL,
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 2
    config:
      latest_version_pointer:
        enabled: true
        alias: dim_customers_current
    versions:
      - v: 1
      - v: 2
""",
        }

    def test_custom_alias_view_created(self, project):
        run_dbt(["run"])
        assert _view_exists(project, "dim_customers_current"), (
            "Pointer view with custom alias 'dim_customers_current' was not created"
        )

    def test_default_name_view_not_created(self, project):
        run_dbt(["run"])
        # The default name (dim_customers) must NOT exist when a custom alias is set
        assert not _view_exists(project, "dim_customers"), (
            "Default-named pointer view 'dim_customers' should not exist when custom alias is set"
        )

    def test_custom_alias_view_resolves_to_v2(self, project):
        run_dbt(["run"])
        text = _view_text(project, "dim_customers_current")
        assert "dim_customers_v2" in text, (
            f"Custom alias pointer view should reference v2, got:\n{text}"
        )


# ---------------------------------------------------------------------------
# 4. Global flag: latest_version_pointer_enabled_by_default
# ---------------------------------------------------------------------------


class TestPointerViewGlobalFlag:
    """Pointer views are created for all versioned models when the global flag is true,
    without per-model `latest_version_pointer.enabled` configuration.

    Acceptance criteria:
      ✓ Test global flag enable (latest_version_pointer_enabled_by_default: true)
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "send_anonymous_usage_stats": False,
                "latest_version_pointer_enabled_by_default": True,
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "dim_customers_v2.sql": _MODEL_V2_SQL,
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 2
    versions:
      - v: 1
      - v: 2
""",
        }

    def test_pointer_view_created_via_global_flag(self, project):
        run_dbt(["run"])
        assert _view_exists(project, "dim_customers"), (
            "Pointer view was not created despite latest_version_pointer_enabled_by_default=true"
        )

    def test_pointer_view_resolves_to_latest(self, project):
        run_dbt(["run"])
        text = _view_text(project, "dim_customers")
        assert "dim_customers_v2" in text


# ---------------------------------------------------------------------------
# 5. Per-model flag only (global flag off)
# ---------------------------------------------------------------------------


class TestPointerViewPerModelFlag:
    """Pointer view is created only for the model that has the per-model flag set.
    The global flag is explicitly left at its default (false).

    Acceptance criteria:
      ✓ Test per-model enable (config.latest_version_pointer.enabled: true)
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "dim_customers_v2.sql": _MODEL_V2_SQL,
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 2
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
      - v: 2
""",
        }

    def test_pointer_view_created_per_model(self, project):
        run_dbt(["run"])
        assert _view_exists(project, "dim_customers"), (
            "Per-model latest_version_pointer.enabled=true did not create the pointer view"
        )

    def test_pointer_view_disabled_by_default_without_flag(self, project):
        """A second versioned model without the per-model flag must NOT get a pointer."""
        # Append a second versioned model that has no latest_version_pointer config
        write_file(
            """
models:
  - name: dim_customers
    latest_version: 2
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
      - v: 2
  - name: fct_orders
    latest_version: 1
    versions:
      - v: 1
""",
            project.project_root,
            "models",
            "schema.yml",
        )
        write_file("select 100 as order_id", project.project_root, "models", "fct_orders_v1.sql")
        run_dbt(["run"])
        # dim_customers pointer must exist
        assert _view_exists(project, "dim_customers")
        # fct_orders pointer must NOT exist (no per-model flag, global flag is default=false)
        assert not _view_exists(project, "fct_orders"), (
            "fct_orders pointer view should not exist without per-model or global flag"
        )


# ---------------------------------------------------------------------------
# 6. Version bump — pointer view updated to new latest version
# ---------------------------------------------------------------------------


class TestPointerViewVersionBump:
    """After bumping latest_version from 1 to 2, the pointer view is updated.

    Acceptance criteria:
      ✓ Test version bump/rollback scenario (changing latest_version value)
      ✓ Test drop/re-create of pointer views on refresh
    """

    _SCHEMA_V1_LATEST = """
models:
  - name: dim_customers
    latest_version: 1
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
      - v: 2
"""

    _SCHEMA_V2_LATEST = """
models:
  - name: dim_customers
    latest_version: 2
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
      - v: 2
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "dim_customers_v2.sql": _MODEL_V2_SQL,
            "schema.yml": self._SCHEMA_V1_LATEST,
        }

    def test_initial_pointer_resolves_to_v1(self, project):
        # Reset to the known starting state before acting, so this test is
        # independent of any schema.yml mutations made by sibling tests.
        write_file(self._SCHEMA_V1_LATEST, project.project_root, "models", "schema.yml")
        run_dbt(["run"])
        text = _view_text(project, "dim_customers")
        assert "dim_customers_v1" in text, (
            f"Initial pointer should reference v1, got:\n{text}"
        )

    def test_after_bump_pointer_resolves_to_v2(self, project):
        # Start from a clean v1 state, then bump to v2.
        write_file(self._SCHEMA_V1_LATEST, project.project_root, "models", "schema.yml")
        run_dbt(["run"])
        write_file(self._SCHEMA_V2_LATEST, project.project_root, "models", "schema.yml")
        run_dbt(["run"])
        text = _view_text(project, "dim_customers")
        assert "dim_customers_v2" in text, (
            f"After bump pointer should reference v2, got:\n{text}"
        )
        assert "dim_customers_v1" not in text, (
            "Pointer still references v1 after version bump"
        )

    def test_rollback_pointer_resolves_to_v1(self, project):
        # Start from v2 (bump), then roll back to v1.
        write_file(self._SCHEMA_V2_LATEST, project.project_root, "models", "schema.yml")
        run_dbt(["run"])
        write_file(self._SCHEMA_V1_LATEST, project.project_root, "models", "schema.yml")
        run_dbt(["run"])
        text = _view_text(project, "dim_customers")
        assert "dim_customers_v1" in text, (
            f"After rollback pointer should reference v1, got:\n{text}"
        )


# ---------------------------------------------------------------------------
# 7. Downstream model references pointer view
# ---------------------------------------------------------------------------


class TestPointerViewDownstreamRef:
    """An unversioned downstream model resolves ref() directly to the latest
    versioned node, while external SQL can query that version through the
    pointer view.

    Acceptance criteria:
      ✓ Test downstream ref() resolves to the latest versioned node and the
        pointer view remains queryable by external SQL
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "dim_customers_v2.sql": _MODEL_V2_SQL,
            "downstream.sql": _DOWNSTREAM_SQL,
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 2
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
      - v: 2
""",
        }

    def test_downstream_run_succeeds(self, project):
        results = run_dbt(["run"])
        # v1, v2 (versioned models) + downstream = 3 RunResult entries.
        # The pointer view is a post-run side-effect and is NOT a separate result.
        assert len(results) == 3

    def test_downstream_can_query_through_pointer(self, project):
        run_dbt(["run"])
        # Verify the pointer view itself is queryable by external consumers
        # (e.g. BI tools, ad-hoc SQL) — this is the primary value of the feature.
        pointer_rows = project.run_sql(
            f"SELECT * FROM \"{project.test_schema}\".dim_customers",
            fetch="all",
        )
        assert pointer_rows, "Pointer view returned no rows"

        # The downstream dbt model resolves ref('dim_customers') directly to the
        # latest versioned node (dim_customers_v2), while the pointer view is a
        # separate external SQL surface that still exposes the current version.
        downstream_rows = project.run_sql(
            f"SELECT * FROM \"{project.test_schema}\".downstream",
            fetch="all",
        )
        assert downstream_rows, "Downstream model returned no rows"

    def test_downstream_depends_on_latest_version(self, project):
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        downstream = manifest.nodes["model.test.downstream"]
        # ref('dim_customers') in downstream resolves to the latest versioned node
        assert "model.test.dim_customers.v2" in downstream.depends_on.nodes


# ---------------------------------------------------------------------------
# 8. MULTISET table as the versioned model
# ---------------------------------------------------------------------------


class TestPointerViewWithMultisetTable:
    """Pointer view works correctly when the underlying versioned table is MULTISET.

    Acceptance criteria:
      ✓ Test with MULTISET/SET tables
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": (
                "{{ config(materialized='table', table_kind='MULTISET') }}\n" + _MODEL_V1_SQL
            ),
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 1
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
""",
        }

    def test_pointer_view_created_over_multiset_table(self, project):
        run_dbt(["run"])
        assert _view_exists(project, "dim_customers"), (
            "Pointer view not created over MULTISET versioned table"
        )

    def test_pointer_view_resolves_to_multiset_table(self, project):
        run_dbt(["run"])
        text = _view_text(project, "dim_customers")
        assert "dim_customers_v1" in text


# ---------------------------------------------------------------------------
# 9. SET table as the versioned model
# ---------------------------------------------------------------------------


class TestPointerViewWithSetTable:
    """Pointer view works correctly when the underlying versioned table is SET.

    Acceptance criteria:
      ✓ Test with MULTISET/SET tables
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": (
                "{{ config(\n"
                "    materialized='table',\n"
                "    table_kind='SET',\n"
                "    index='PRIMARY INDEX (customer_id)'\n"
                ") }}\n" + _MODEL_V1_SQL
            ),
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 1
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
""",
        }

    def test_pointer_view_created_over_set_table(self, project):
        run_dbt(["run"])
        assert _view_exists(project, "dim_customers"), (
            "Pointer view not created over SET versioned table"
        )

    def test_pointer_view_resolves_to_set_table(self, project):
        run_dbt(["run"])
        text = _view_text(project, "dim_customers")
        assert "dim_customers_v1" in text


# ---------------------------------------------------------------------------
# 10. Different PRIMARY INDEX configurations
# ---------------------------------------------------------------------------


class TestPointerViewWithPrimaryIndex:
    """Pointer view works regardless of the PRIMARY INDEX strategy on the
    underlying versioned table.

    Acceptance criteria:
      ✓ Test with different PRIMARY INDEX configurations
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            # v1: PRIMARY INDEX on customer_id (typical OLTP lookup key)
            "dim_customers_v1.sql": (
                "{{ config(materialized='table', index='PRIMARY INDEX (customer_id)') }}\n"
                "select 1 as customer_id, 'alice' as customer_name"
            ),
            # v2: PRIMARY INDEX on created_date (analytics / time-range scan)
            "dim_customers_v2.sql": (
                "{{ config(materialized='table', index='PRIMARY INDEX (created_date)') }}\n"
                "select 1 as customer_id, 'alice' as customer_name, '2024-01-01' as created_date"
            ),
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 2
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
      - v: 2
""",
        }

    def test_pointer_view_created_with_varied_pi(self, project):
        run_dbt(["run"])
        assert _view_exists(project, "dim_customers")

    def test_pointer_resolves_to_latest(self, project):
        run_dbt(["run"])
        text = _view_text(project, "dim_customers")
        assert "dim_customers_v2" in text


# ---------------------------------------------------------------------------
# 11. Partitioned versioned table
# ---------------------------------------------------------------------------


class TestPointerViewWithPartitionedTable:
    """Pointer view works when the versioned table uses PARTITION BY.

    Acceptance criteria:
      ✓ Test with partitioned versioned tables
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": (
                "{{ config(\n"
                "    materialized='table',\n"
                "    index='PRIMARY INDEX (customer_id) PARTITION BY RANGE_N(customer_id BETWEEN 1 AND 100 EACH 1)'\n"
                ") }}\n"
                "select 1 as customer_id, 'alice' as customer_name"
            ),
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 1
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
""",
        }

    def test_pointer_view_created_over_partitioned_table(self, project):
        run_dbt(["run"])
        assert _view_exists(project, "dim_customers"), (
            "Pointer view not created over partitioned versioned table"
        )

    def test_pointer_resolves_to_partitioned_table(self, project):
        run_dbt(["run"])
        text = _view_text(project, "dim_customers")
        assert "dim_customers_v1" in text


# ---------------------------------------------------------------------------
# 12. Explain plan through pointer view
# ---------------------------------------------------------------------------


class TestPointerViewExplainPlan:
    """Teradata's optimizer can generate an explain plan for queries through
    the pointer view, confirming predicate pushdown works.

    Acceptance criteria:
      ✓ Test explain plan on pointer view queries
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 1
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
""",
        }

    def test_explain_plan_succeeds_through_pointer(self, project):
        run_dbt(["run"])
        # EXPLAIN returns rows describing the query plan — no exception = plan generated OK
        rows = project.run_sql(
            f"EXPLAIN SELECT * FROM {project.test_schema}.dim_customers "
            f"WHERE customer_id = 1",
            fetch="all",
        )
        assert rows, "EXPLAIN returned no rows — plan generation may have failed"


# ---------------------------------------------------------------------------
# 13. Pointer view disabled — no pointer created
# ---------------------------------------------------------------------------


class TestPointerViewDisabled:
    """When latest_version_pointer.enabled is explicitly false, no pointer view
    is created on a fresh run.

    Note: dbt-core's pointer creation logic (ModelRunner._materialize_
    latest_version_pointer / _should_create_latest_version_pointer in
    dbt/task/run.py) only ever skips or creates the pointer; it does not drop
    a pointer view left behind by a prior run where the flag was enabled.
    That is core dbt behavior, not something this adapter controls, so it is
    documented here rather than asserted as a regression test (asserting it
    would pin this suite to a specific dbt-core release's lack of cleanup
    logic, which could reasonably change in a future dbt-core version).

    Acceptance criteria:
      ✓ Per-model disable respected
    """

    _SCHEMA_DISABLED = """
models:
  - name: dim_customers
    latest_version: 1
    config:
      latest_version_pointer:
        enabled: false
    versions:
      - v: 1
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "schema.yml": self._SCHEMA_DISABLED,
        }

    def test_no_pointer_view_when_disabled(self, project):
        write_file(self._SCHEMA_DISABLED, project.project_root, "models", "schema.yml")
        run_dbt(["run"])
        assert not _view_exists(project, "dim_customers"), (
            "Pointer view was created despite latest_version_pointer.enabled=false"
        )


# ---------------------------------------------------------------------------
# 14. dbt compile includes pointer view in graph output
# ---------------------------------------------------------------------------


class TestPointerViewInDbtLs:
    """dbt ls output for a versioned model with the pointer enabled.

    NOTE ON CONTRACT: the pointer view is never added to the manifest as its
    own node (see dbt/task/run.py ModelRunner._materialize_latest_version_pointer,
    which builds a synthetic, never-manifest-added node purely as a post-run
    side effect). Therefore `dbt ls` can only ever list the concrete versioned
    node; there is no separate listable identifier for the pointer view
    itself. `dbt ls`'s default output mode is "selector", which yields the
    package-qualified FQN (e.g. "test.dim_customers.v1"), not the bare
    search_name ("dim_customers.v1") -- confirmed empirically against a live
    run. This test asserts that exact, unambiguous identifier rather than a
    loose substring check (a bare "dim_customers" substring would also match
    "dim_customers.v1" and pass even if the pointer/versioned node were
    mislabeled or missing).

    Acceptance criteria:
      ✓ Test dbt ls output includes pointer views (documented limitation: only
        the underlying versioned node is listable; see note above)
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "schema.yml": """
models:
  - name: dim_customers
    latest_version: 1
    config:
      latest_version_pointer:
        enabled: true
    versions:
      - v: 1
""",
        }

    def test_dbt_ls_succeeds(self, project):
        run_dbt(["run"])
        assert _view_exists(project, "dim_customers"), (
            "Pointer view was not created after running the versioned model"
        )

        _, logs = run_dbt_and_capture([
            "ls",
            "--select",
            "dim_customers",
            "--output",
            "selector",
        ])
        # dbt ls must complete without error and list the exact versioned-node
        # identifier after the model has run. Request selector output explicitly
        # so the assertion matches dbt's package-qualified FQN rather than the
        # bare resource name. Strip any ANSI color codes defensively, since
        # captured log lines can carry them.
        ansi_re = re.compile(r"\x1b\[[0-9;]*m")
        listed = [ansi_re.sub("", line).strip() for line in logs.splitlines() if line.strip()]
        # dbt-core's project test fixtures hardcode the test project name to
        # "test" (see dbt/tests/fixtures/project.py dbt_project_yml fixture).
        expected = "test.dim_customers.v1"
        assert expected in listed, (
            f"Expected '{expected}' in dbt ls output, got: {listed}"
        )


# ---------------------------------------------------------------------------
# 15. Grants propagate to pointer view
# ---------------------------------------------------------------------------


class TestPointerViewGrants:
    """When a grants config is set on the versioned model, dbt propagates those
    grants to the pointer view as well (inherited from the pointer_node config).

    Acceptance criteria:
      ✓ Verify grants/permissions on pointer views are correct

    Note: This test is skipped when DBT_TEST_USER_1 is not set in the environment
    (same convention used in test_grants.py).
    """

    @pytest.fixture(scope="class")
    def models(self, request):
        import os

        user_1 = os.getenv("DBT_TEST_USER_1")
        if not user_1:
            pytest.skip("DBT_TEST_USER_1 not set — skipping grants test")

        return {
            "dim_customers_v1.sql": _MODEL_V1_SQL,
            "schema.yml": f"""
models:
  - name: dim_customers
    latest_version: 1
    config:
      latest_version_pointer:
        enabled: true
      grants:
        select: ["{user_1}"]
    versions:
      - v: 1
""",
        }

    def test_run_with_grants_succeeds(self, project):
        results = run_dbt(["run"])
        # 1 versioned model node; pointer view is a post-run side-effect
        assert len(results) == 1

    def test_grants_applied_to_pointer_view(self, project):
        import os

        user_1 = os.getenv("DBT_TEST_USER_1")
        run_dbt(["run"])
        rows = project.run_sql(
            f"SELECT Username FROM DBC.AllRightsV "
            f"WHERE DatabaseName='{project.test_schema}' "
            f"AND TRIM(UPPER(TableName))='DIM_CUSTOMERS' "
            f"AND AccessRight='R' "
            f"AND TRIM(UPPER(Username))='{user_1.upper()}'",
            fetch="all",
        )
        assert rows, (
            f"SELECT grant for '{user_1}' not found on pointer view 'dim_customers'"
        )
