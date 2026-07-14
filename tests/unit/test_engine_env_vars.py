"""Unit tests for IDE-26224: DBT_ENGINE_* env vars compatibility.

dbt Core 1.11 renamed two specific environment variables used for state
artifact lookup:
  - DBT_STATE       → DBT_ENGINE_STATE
  - DBT_DEFER_STATE → DBT_ENGINE_DEFER_STATE

These tests verify that the Teradata adapter credentials and configuration
are completely unaffected by both the old and new variable names being
present in the environment.

Coverage also includes the broader DBT_ENGINE_* namespace (DBT_ENGINE_FULL_REFRESH,
DBT_ENGINE_TARGET, DBT_ENGINE_PROFILES_DIR) because dbt-core may introduce
additional variables in this prefix over time. Testing each now ensures
the adapter remains isolated from the entire namespace, not just the two
renamed vars specific to IDE-26224.

The adapter itself never reads these vars; they are handled exclusively by
dbt-core. The tests here guard against accidental coupling.
"""

import os
from unittest.mock import patch

from dbt.adapters.teradata.connections import TeradataCredentials


def _make_credentials(**overrides):
    defaults = {
        "server": "localhost",
        "schema": "test_schema",
        "username": "test_user",
        "password": "test_pass",
    }
    defaults.update(overrides)
    return TeradataCredentials(**defaults)


class TestEngineEnvVarsIsolation:
    """DBT_ENGINE_* env vars (dbt 1.11) must not affect adapter credential setup."""

    def test_credentials_unaffected_by_dbt_engine_state(self):
        with patch.dict(os.environ, clear=True, values={"DBT_ENGINE_STATE": "/some/path/to/state"}):
            creds = _make_credentials()
            assert creds.server == "localhost"
            assert creds.username == "test_user"

    def test_credentials_unaffected_by_dbt_engine_defer_state(self):
        with patch.dict(os.environ, clear=True, values={"DBT_ENGINE_DEFER_STATE": "/some/path/to/defer"}):
            creds = _make_credentials()
            assert creds.server == "localhost"
            assert creds.schema == "test_schema"

    def test_credentials_unaffected_by_dbt_engine_full_refresh(self):
        with patch.dict(os.environ, clear=True, values={"DBT_ENGINE_FULL_REFRESH": "true"}):
            creds = _make_credentials()
            assert creds.server == "localhost"

    def test_credentials_unaffected_by_dbt_engine_target(self):
        with patch.dict(os.environ, clear=True, values={"DBT_ENGINE_TARGET": "prod"}):
            creds = _make_credentials()
            assert creds.server == "localhost"

    def test_credentials_unaffected_by_dbt_engine_profiles_dir(self):
        with patch.dict(os.environ, clear=True, values={"DBT_ENGINE_PROFILES_DIR": "/some/profiles"}):
            creds = _make_credentials()
            assert creds.server == "localhost"

    def test_credentials_unaffected_by_multiple_engine_vars(self):
        engine_vars = {
            "DBT_ENGINE_STATE": "/path/state",
            "DBT_ENGINE_DEFER_STATE": "/path/defer",
            "DBT_ENGINE_FULL_REFRESH": "true",
            "DBT_ENGINE_TARGET": "prod",
            "DBT_ENGINE_PROFILES_DIR": "/path/profiles",
        }
        with patch.dict(os.environ, engine_vars, clear=True):
            creds = _make_credentials()
            assert creds.server == "localhost"
            assert creds.schema == "test_schema"
            assert creds.username == "test_user"


class TestLegacyStateEnvVarsIsolation:
    """Old DBT_STATE and DBT_DEFER_STATE env vars must not break adapter credential setup."""

    def test_credentials_unaffected_by_dbt_state(self):
        with patch.dict(os.environ, clear=True, values={"DBT_STATE": "/some/path/to/state"}):
            creds = _make_credentials()
            assert creds.server == "localhost"

    def test_credentials_unaffected_by_dbt_defer_state(self):
        with patch.dict(os.environ, clear=True, values={"DBT_DEFER_STATE": "/some/path/to/defer"}):
            creds = _make_credentials()
            assert creds.server == "localhost"


class TestOldAndNewEnvVarsCoexist:
    """DBT_STATE / DBT_DEFER_STATE and their DBT_ENGINE_* replacements can coexist without breaking the adapter."""

    def test_both_state_vars_coexist(self):
        env_vars = {
            "DBT_STATE": "/old/path",
            "DBT_ENGINE_STATE": "/new/path",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            creds = _make_credentials()
            assert creds.server == "localhost"
            assert creds.username == "test_user"

    def test_old_and_new_defer_coexist(self):
        env_vars = {
            "DBT_DEFER_STATE": "/old/defer",
            "DBT_ENGINE_DEFER_STATE": "/new/defer",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            creds = _make_credentials()
            assert creds.server == "localhost"


class TestTeradataEnvVarsDoNotConflictWithEngineVars:
    """Teradata-specific env vars and DBT_ENGINE_* vars can coexist."""

    def test_explicit_credentials_not_overridden_by_engine_vars(self):
        """Explicitly passed credentials must not be silently overridden by
        DBT_ENGINE_* or DBT_TERADATA_* env vars present in the environment."""
        env_vars = {
            "DBT_ENGINE_STATE": "/some/path",
            "DBT_ENGINE_TARGET": "prod",
            "DBT_TERADATA_SERVER_NAME": "env-host.example.com",
            "DBT_TERADATA_USERNAME": "env_user",
            "DBT_TERADATA_PASSWORD": "env_pass",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            creds = _make_credentials(
                server="explicit-host.example.com",
                username="explicit_user",
                password="explicit_pass",
            )
            assert creds.server == "explicit-host.example.com"
            assert creds.username == "explicit_user"
            assert creds.password == "explicit_pass"
            assert creds.server != os.environ["DBT_TERADATA_SERVER_NAME"]
            assert creds.username != os.environ["DBT_TERADATA_USERNAME"]

    def test_teradata_env_vars_not_auto_read_by_credentials(self):
        """DBT_TERADATA_* env vars must not be auto-consumed by TeradataCredentials.

        The adapter reads credentials from the dbt profile, not from env vars
        directly. Setting DBT_TERADATA_* vars in the environment must not silently
        change the credential values when defaults are used.
        """
        env_vars = {
            "DBT_TERADATA_SERVER_NAME": "env-host.example.com",
            "DBT_TERADATA_USERNAME": "env_user",
            "DBT_TERADATA_PASSWORD": "env_pass",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            creds = _make_credentials()
            # Defaults must be used, not the env var values
            assert creds.server == "localhost"
            assert creds.username == "test_user"
            assert creds.password == "test_pass"

    def test_credentials_with_all_env_var_types_present(self):
        env_vars = {
            "DBT_STATE": "/old/state",
            "DBT_ENGINE_STATE": "/new/state",
            "DBT_TERADATA_SERVER_NAME": "env-host.example.com",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            creds = _make_credentials(server="explicit-host.example.com")
            assert creds.server == "explicit-host.example.com"
            assert creds.server != os.environ["DBT_TERADATA_SERVER_NAME"]
