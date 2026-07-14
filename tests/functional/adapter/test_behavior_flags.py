"""Functional tests for IDE-26230: dbt 1.11 legacy-behavior change flags.

dbt Core 1.11 introduces two opt-in behavior change flags in dbt_project.yml:

  require_unique_project_resource_names (default: false)
    false → duplicate names across resource types (e.g. model + seed) emit a
            DuplicateNameDistinctNodeTypesDeprecation warning but do not fail.
    true  → raises DuplicateResourceNameError immediately.

  require_ref_searches_node_package_before_root (default: false)
    false → when resolving ref() in a package model, dbt searches the root
            project first, then the defining package.
    true  → dbt searches the defining package first, then the root project.

These are pure dbt-core behaviors; the adapter is a pass-through. The tests
confirm the Teradata adapter does not obstruct or alter these flag semantics.

All tests are functional and require a live Teradata Vantage instance.
All tests in this module are skipped on dbt-core < 1.11 (module-level pytestmark).
"""

import pytest
import dbt.version as _dbt_version
from packaging.version import Version

from dbt.exceptions import DuplicateResourceNameError
from dbt.tests.util import run_dbt, run_dbt_and_capture

_dbt_version_tuple = Version(_dbt_version.__version__).release[:2]

pytestmark = pytest.mark.skipif(
    _dbt_version_tuple < (1, 11),
    reason=f"Requires dbt-core >= 1.11 (found {_dbt_version.__version__})",
)


# ── fixtures ──────────────────────────────────────────────────────────


_SEED_CSV = """id,name
1,alice
2,bob
""".strip()

_MODEL_SIMPLE_SQL = "select 1 as id, 'foo' as name"

# Model that shares its resource NAME with the seed above but has a different
# alias, so only the name-uniqueness check fires (not the alias-collision check).
# The alias config prevents AmbiguousAliasError so we can observe the
# require_unique_project_resource_names flag behaviour in isolation.
_MODEL_DUPLICATE_SQL = "{{ config(alias='shared_resource_view') }}\nselect 99 as id, 'dup' as name"

# A harmless unique model used as a sanity-check baseline.
# Note: 'result' is a reserved word in Teradata, so use 'val' instead.
_MODEL_UNIQUE_SQL = "select 42 as val"


# ── require_unique_project_resource_names: flag = false (default) ─────


class TestRequireUniqueProjectResourceNamesDefault:
    """
    With require_unique_project_resource_names=false a seed and a model sharing
    the same name must not prevent compilation. dbt emits a deprecation warning
    but continues successfully.

    Requires dbt >= 1.11: earlier versions always error on duplicate resource names
    regardless of any flag.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"shared_resource.csv": _SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "shared_resource.sql": _MODEL_DUPLICATE_SQL,
            "unique_model.sql": _MODEL_UNIQUE_SQL,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "send_anonymous_usage_stats": False,
                "require_unique_project_resource_names": False,
            }
        }

    def test_compile_succeeds_with_duplicate_names(self, project):
        results = run_dbt(["compile"])
        assert results is not None

    def test_deprecation_warning_emitted(self, project):
        _, logs = run_dbt_and_capture(["--debug", "compile"])
        assert "DuplicateNameDistinctNodeTypesDeprecation" in logs


# ── require_unique_project_resource_names: flag = true ────────────────


class TestRequireUniqueProjectResourceNamesEnabled:
    """
    With require_unique_project_resource_names=true a model and seed sharing
    the same name must raise an error and stop compilation.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"shared_resource.csv": _SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {"shared_resource.sql": _MODEL_DUPLICATE_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "send_anonymous_usage_stats": False,
                "require_unique_project_resource_names": True,
            }
        }

    def test_compile_fails_with_duplicate_names(self, project):
        with pytest.raises(DuplicateResourceNameError):
            run_dbt(["compile"])


# ── require_unique_project_resource_names: no duplicates → always ok ──


class TestRequireUniqueProjectResourceNamesNoDuplicates:
    """
    When there are no duplicate resource names the flag value (true or false)
    must not affect the outcome — compilation always succeeds.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": _MODEL_SIMPLE_SQL,
            "model_b.sql": _MODEL_UNIQUE_SQL,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "send_anonymous_usage_stats": False,
                "require_unique_project_resource_names": True,
            }
        }

    def test_compile_succeeds_with_no_duplicates(self, project):
        results = run_dbt(["compile"])
        assert results is not None

    def test_run_succeeds_with_no_duplicates(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2


# ── require_ref_searches_node_package_before_root: flag accepted ───────


_MODEL_REF_SQL = "select * from {{ ref('model_a') }}"


class TestRefSearchNodePackageBeforeRootFlagFalse:
    """
    With require_ref_searches_node_package_before_root=false (default) dbt
    resolves ref() by searching the root project first. This test confirms
    the flag is accepted and compilation succeeds.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": _MODEL_SIMPLE_SQL,
            "model_b.sql": _MODEL_REF_SQL,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "send_anonymous_usage_stats": False,
                "require_ref_searches_node_package_before_root": False,
            }
        }

    def test_compile_succeeds_flag_false(self, project):
        results = run_dbt(["compile"])
        assert results is not None

    def test_run_succeeds_flag_false(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2


class TestRefSearchNodePackageBeforeRootFlagTrue:
    """
    With require_ref_searches_node_package_before_root=true dbt searches the
    defining package before the root project when resolving ref(). Within a
    single-package project (no packages.yml) the behaviour is identical to
    flag=false — all refs resolve normally and compilation succeeds.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": _MODEL_SIMPLE_SQL,
            "model_b.sql": _MODEL_REF_SQL,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "send_anonymous_usage_stats": False,
                "require_ref_searches_node_package_before_root": True,
            }
        }

    def test_compile_succeeds_flag_true(self, project):
        results = run_dbt(["compile"])
        assert results is not None

    def test_run_succeeds_flag_true(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2


# ── both flags enabled together ────────────────────────────────────────


class TestBothFlagsEnabledTogether:
    """
    Both behavior flags can be set simultaneously without conflict.
    A clean project (no duplicate names, no package ambiguity) must
    compile and run successfully with both flags set to true.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": _MODEL_SIMPLE_SQL,
            "model_b.sql": _MODEL_REF_SQL,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "send_anonymous_usage_stats": False,
                "require_unique_project_resource_names": True,
                "require_ref_searches_node_package_before_root": True,
            }
        }

    def test_compile_succeeds_with_both_flags(self, project):
        results = run_dbt(["compile"])
        assert results is not None

    def test_run_succeeds_with_both_flags(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
