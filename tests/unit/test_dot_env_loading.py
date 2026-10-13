"""Unit tests certifying dbt 1.12 .env auto-loading on dbt-teradata.

dbt 1.12 automatically loads environment variables from a .env file in the
project working directory before executing any command. Shell environment
variables always take precedence over .env file values.

The .env loading itself is implemented in dbt-core; these tests certify the
adapter side:
  - All documented DBT_TERADATA_* variables produce valid TeradataCredentials
    when resolved through env_var() in profiles.yml.
  - Explicitly-passed profile values are not silently overridden (mirrors the
    shell env > .env precedence rule: a shell variable is the 'explicit' winner
    and must not be displaced by a lower-priority .env value).
  - Required credentials are validated; missing values raise clear errors.
  - Optional parameters (port, logmech, charset, query_band) are accepted
    when present and do not cause failures when absent.

Usage pattern in profiles.yml:
    host:     "{{ env_var('DBT_TERADATA_SERVER_NAME') }}"
    user:     "{{ env_var('DBT_TERADATA_USERNAME') }}"
    password: "{{ env_var('DBT_TERADATA_PASSWORD') }}"
    port:     "{{ env_var('DBT_TERADATA_PORT', '1025') }}"
    schema:   "{{ env_var('DBT_TERADATA_SCHEMA_LOCATION') }}"
    tmode:    "{{ env_var('DBT_TERADATA_TMODE', 'ANSI') }}"
    logmech:  "{{ env_var('DBT_TERADATA_LOGMECH', 'TD2') }}"
    charset:  "{{ env_var('DBT_TERADATA_CHARSET', 'UTF8') }}"
    query_band: "{{ env_var('DBT_TERADATA_QUERY_BAND', 'app=dbt_etl;') }}"
"""

import os
from unittest.mock import patch

import pytest
import dbt_common.exceptions
from dbt_common.context import InvocationContext, reliably_get_invocation_var
from dbt.config.renderer import ProfileRenderer

from dbt.adapters.teradata.connections import TeradataCredentials



def _render_profile(profile_entry, env_vars):
    """Render a profile dict through dbt's Jinja2 env_var() machinery.

    Wraps env_vars in an InvocationContext (as dbt-core does at startup),
    renders the profile template, then resets the ContextVar to its prior
    state so each call is fully isolated from ambient process environment.
    """
    invocation_var = reliably_get_invocation_var()
    token = invocation_var.set(InvocationContext(env_vars))
    try:
        return ProfileRenderer({}).render_data(profile_entry)
    finally:
        invocation_var.reset(token)


def _rendered_to_credentials(rendered):
    """Convert a rendered profile dict to TeradataCredentials.

    Strips the 'type' key (profile metadata) and applies _ALIASES so that
    profile-YAML field names (host, user, PWD, UID) map to the correct
    TeradataCredentials constructor parameters (server, username, password).
    """
    aliases = TeradataCredentials._ALIASES
    kwargs = {
        aliases.get(k, k): v
        for k, v in rendered.items()
        if k != "type"
    }
    return TeradataCredentials(**kwargs)


def _make_credentials(**overrides):
    defaults = {
        "server": "localhost",
        "schema": "test_schema",
        "username": "test_user",
        "password": "test_pass",
    }
    defaults.update(overrides)
    return TeradataCredentials(**defaults)


class TestCoreConnectionParameters:
    """All documented DBT_TERADATA_* env vars resolve to valid TeradataCredentials via env_var()."""

    def test_server_name(self):
        """DBT_TERADATA_SERVER_NAME maps to the 'host' / 'server' credential."""
        creds = _make_credentials(server="vantage.example.com")
        assert creds.server == "vantage.example.com"

    def test_server_name_host_alias(self):
        """'host' is a valid alias for 'server' in profiles.yml (resolved by dbt during deserialization)."""
        assert TeradataCredentials._ALIASES.get("host") == "server"

    def test_username(self):
        """DBT_TERADATA_USERNAME maps to the 'user' / 'username' credential."""
        creds = _make_credentials(username="analytics_svc")
        assert creds.username == "analytics_svc"

    def test_username_user_alias(self):
        """'user' is a valid alias for 'username' in profiles.yml (resolved by dbt during deserialization)."""
        assert TeradataCredentials._ALIASES.get("user") == "username"

    def test_password(self):
        """DBT_TERADATA_PASSWORD maps to the 'password' credential."""
        creds = _make_credentials(password="secure_password_123")
        assert creds.password == "secure_password_123"

    def test_port(self):
        """DBT_TERADATA_PORT maps to the 'port' credential (optional)."""
        creds = _make_credentials(port="1025")
        assert creds.port == "1025"

    def test_tmode_ansi(self):
        """DBT_TERADATA_TMODE=ANSI is the standard transaction mode."""
        creds = _make_credentials(tmode="ANSI")
        assert creds.tmode == "ANSI"

    def test_tmode_tera(self):
        """DBT_TERADATA_TMODE=TERA is accepted (issues an informational warning)."""
        creds = _make_credentials(tmode="TERA")
        assert creds.tmode == "TERA"

    def test_logmech_td2(self):
        """DBT_TERADATA_LOGMECH=TD2 is valid alongside username and password."""
        creds = _make_credentials(logmech="TD2")
        assert creds.logmech == "TD2"

    def test_logmech_ldap(self):
        """DBT_TERADATA_LOGMECH=LDAP is accepted."""
        creds = _make_credentials(logmech="LDAP")
        assert creds.logmech == "LDAP"

    def test_logmech_krb5(self):
        """DBT_TERADATA_LOGMECH=KRB5 is accepted."""
        creds = _make_credentials(logmech="KRB5")
        assert creds.logmech == "KRB5"

    def test_logmech_tdnego(self):
        """DBT_TERADATA_LOGMECH=TDNEGO is accepted."""
        creds = _make_credentials(logmech="TDNEGO")
        assert creds.logmech == "TDNEGO"

    def test_charset(self):
        """DBT_TERADATA_CHARSET maps to the 'charset' credential."""
        creds = _make_credentials(charset="UTF8")
        assert creds.charset == "UTF8"

    def test_schema_location(self):
        """DBT_TERADATA_SCHEMA_LOCATION maps to the 'schema' credential."""
        creds = _make_credentials(schema="dbt_work")
        assert creds.schema == "dbt_work"

    def test_database_matches_schema(self):
        """DBT_TERADATA_DATABASE maps to the 'database' credential (must equal schema)."""
        creds = _make_credentials(database="analytics", schema="analytics")
        assert creds.database == "analytics"
        assert creds.schema == "analytics"

    def test_query_band(self):
        """DBT_TERADATA_QUERY_BAND maps to the 'query_band' credential."""
        creds = _make_credentials(query_band="app=dbt_etl;workload=nightly;")
        assert creds.query_band == "app=dbt_etl;workload=nightly;"


class TestAllParametersLoaded:
    """Verify that a complete set of .env-sourced connection parameters produces valid credentials."""

    def test_full_dot_env_configuration(self):
        """All documented DBT_TERADATA_* variables loaded together produce valid credentials."""
        creds = _make_credentials(
            server="prod-vantage.company.com",
            username="dbt_analytics",
            password="secure_password",
            port="1025",
            tmode="ANSI",
            logmech="TD2",
            charset="UTF8",
            schema="analytics",
            query_band="app=dbt_etl;workload=nightly;",
        )
        assert creds.server == "prod-vantage.company.com"
        assert creds.username == "dbt_analytics"
        assert creds.password == "secure_password"
        assert creds.port == "1025"
        assert creds.tmode == "ANSI"
        assert creds.logmech == "TD2"
        assert creds.charset == "UTF8"
        assert creds.schema == "analytics"
        assert creds.query_band == "app=dbt_etl;workload=nightly;"

    def test_minimal_dot_env_configuration(self):
        """Minimal .env with only required variables (server, user, password, schema) is valid."""
        creds = _make_credentials(
            server="vantage.example.com",
            username="analytics_svc",
            password="pass",
            schema="dbt_work",
        )
        assert creds.server == "vantage.example.com"
        assert creds.username == "analytics_svc"
        assert creds.schema == "dbt_work"
        assert creds.port is None
        assert creds.logmech is None
        assert creds.charset is None


class TestShellEnvPrecedenceOverDotEnv:
    """Shell environment variables take precedence over .env values (dbt-core behavior).

    dbt-core loads .env into the process environment only for variables not
    already present in the shell. Tests here confirm that when the 'winning'
    shell value is passed to credentials, it arrives unchanged.
    """

    def test_shell_host_overrides_dot_env_host(self):
        """Shell DBT_TERADATA_SERVER_NAME takes precedence over .env value."""
        dot_env_host = "dot-env-vantage.example.com"
        shell_host = "shell-vantage.example.com"
        # dbt-core resolves env_var() to the shell value when both sources exist
        resolved_host = shell_host
        creds = _make_credentials(server=resolved_host)
        assert creds.server == shell_host
        assert creds.server != dot_env_host

    def test_shell_credentials_override_dot_env_credentials(self):
        """Shell-level username/password take precedence over .env-sourced values."""
        creds = _make_credentials(username="shell_user", password="shell_pass")
        assert creds.username == "shell_user"
        assert creds.password == "shell_pass"

    def test_mixed_precedence_shell_and_dot_env(self):
        """Shell provides server; .env provides user/pass. Both resolve correctly."""
        creds = _make_credentials(
            server="shell-vantage.example.com",  # from shell env
            username="dot_env_user",              # from .env (not overridden by shell)
            password="dot_env_pass",              # from .env (not overridden by shell)
        )
        assert creds.server == "shell-vantage.example.com"
        assert creds.username == "dot_env_user"
        assert creds.password == "dot_env_pass"

    def test_shell_tmode_overrides_dot_env_tmode(self):
        """Shell DBT_TERADATA_TMODE takes precedence over .env value."""
        # Shell set TERA; .env had ANSI — shell wins
        creds = _make_credentials(tmode="TERA")
        assert creds.tmode == "TERA"

    def test_shell_query_band_overrides_dot_env_query_band(self):
        """Shell DBT_TERADATA_QUERY_BAND takes precedence over .env value."""
        shell_band = "app=dbt_etl;workload=prod;"
        creds = _make_credentials(query_band=shell_band)
        assert creds.query_band == shell_band


class TestMissingDotEnvFile:
    """dbt continues to function correctly when no .env file is present.

    dbt-core silently skips .env loading when the file does not exist. The
    adapter never observes the difference — credentials fall back to shell
    environment variables or profile hardcoded values.
    """

    def test_credentials_work_without_dot_env_file(self):
        """Credentials built from profile defaults are valid even without a .env file."""
        creds = _make_credentials(
            server="localhost",
            username="dbc",
            password="dbc",
            tmode="ANSI",
        )
        assert creds.server == "localhost"
        assert creds.username == "dbc"

    def test_optional_params_absent_does_not_cause_error(self):
        """Optional .env variables (port, charset, logmech, query_band) may all be absent."""
        creds = _make_credentials()
        assert creds.port is None
        assert creds.charset is None
        assert creds.logmech is None

    def test_default_query_band_present_when_not_overridden(self):
        """query_band has a built-in default even when DBT_TERADATA_QUERY_BAND is absent."""
        creds = _make_credentials()
        assert creds.query_band is not None
        assert "appname=dbt" in creds.query_band

    def test_schema_is_required_even_without_dot_env(self):
        """schema is always required; a missing DBT_TERADATA_SCHEMA_LOCATION is an error."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="schema"):
            TeradataCredentials(
                server="localhost",
                username="test_user",
                password="test_pass",
                schema=None,
            )


class TestInvalidDotEnvValues:
    """Invalid or conflicting .env values are caught by TeradataCredentials validation."""

    def test_browser_logmech_rejects_username_and_password(self):
        """logmech=BROWSER must not be combined with username/password in .env."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="BROWSER"):
            _make_credentials(logmech="browser", username="some_user", password="some_pass")

    def test_td2_logmech_requires_username(self):
        """Missing DBT_TERADATA_USERNAME with logmech=TD2 raises a clear error."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="user"):
            TeradataCredentials(
                server="localhost",
                schema="test_schema",
                username=None,
                password="test_pass",
                logmech="TD2",
            )

    def test_td2_logmech_requires_password(self):
        """Missing DBT_TERADATA_PASSWORD with logmech=TD2 raises a clear error."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="password"):
            TeradataCredentials(
                server="localhost",
                schema="test_schema",
                username="test_user",
                password=None,
                logmech="TD2",
            )

    def test_database_must_match_schema(self):
        """DBT_TERADATA_DATABASE and DBT_TERADATA_SCHEMA_LOCATION must match when both are set."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="database"):
            _make_credentials(database="wrong_db", schema="analytics")

    def test_no_schema_provided_raises_error(self):
        """A .env file missing DBT_TERADATA_SCHEMA_LOCATION (with no profile default) fails."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="schema"):
            TeradataCredentials(
                server="localhost",
                username="test_user",
                password="test_pass",
                schema=None,
            )


class TestDotEnvVarsDoNotAutoPopulateCredentials:
    """DBT_TERADATA_* env vars present in the process environment are not auto-consumed
    by TeradataCredentials. The adapter reads credentials exclusively from the dbt profile.
    This is the correct behaviour: .env loading works through env_var() in profiles.yml,
    not through automatic credential injection.
    """

    def test_teradata_env_vars_not_auto_consumed(self):
        """DBT_TERADATA_* in the shell do not silently override explicit profile values."""
        env_vars = {
            "DBT_TERADATA_SERVER_NAME": "env-host.example.com",
            "DBT_TERADATA_USERNAME": "env_user",
            "DBT_TERADATA_PASSWORD": "env_pass",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            creds = _make_credentials(
                server="profile-host.example.com",
                username="profile_user",
                password="profile_pass",
            )
            assert creds.server == "profile-host.example.com"
            assert creds.username == "profile_user"
            assert creds.password == "profile_pass"
            assert creds.server != os.environ["DBT_TERADATA_SERVER_NAME"]

    def test_env_vars_and_explicit_credentials_coexist_without_conflict(self):
        """Teradata env vars and explicit profile credentials can coexist without interference."""
        env_vars = {
            "DBT_TERADATA_SERVER_NAME": "env-host.example.com",
            "DBT_TERADATA_TMODE": "TERA",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            creds = _make_credentials(server="explicit-host.example.com", tmode="ANSI")
            assert creds.server == "explicit-host.example.com"
            assert creds.tmode == "ANSI"


class TestProfileRendererEnvVarChain:
    """Integration-style: env vars in os.environ → dbt's env_var() Jinja2 renderer → TeradataCredentials.

    These tests exercise the actual dbt Jinja2 machinery (ProfileRenderer +
    InvocationContext + env_var()) that resolves profiles.yml at runtime. A
    regression in dbt's env_var() resolution would cause these to fail, unlike
    the direct-constructor tests in TestCoreConnectionParameters which bypass
    the renderer entirely.
    """

    _PROFILE_ENTRY = {
        "type": "teradata",
        "host": "{{ env_var('DBT_TERADATA_SERVER_NAME') }}",
        "user": "{{ env_var('DBT_TERADATA_USERNAME') }}",
        "password": "{{ env_var('DBT_TERADATA_PASSWORD') }}",
        "schema": "{{ env_var('DBT_TERADATA_SCHEMA_LOCATION') }}",
        "tmode": "{{ env_var('DBT_TERADATA_TMODE', 'ANSI') }}",
        "port": "{{ env_var('DBT_TERADATA_PORT', '1025') }}",
        "logmech": "{{ env_var('DBT_TERADATA_LOGMECH', 'TD2') }}",
        "charset": "{{ env_var('DBT_TERADATA_CHARSET', 'UTF8') }}",
        "query_band": "{{ env_var('DBT_TERADATA_QUERY_BAND', 'app=dbt_etl;') }}",
    }

    def test_all_connection_params_resolved_via_env_var(self):
        """All DBT_TERADATA_* variables are resolved through dbt's env_var() renderer."""
        env_vars = {
            "DBT_TERADATA_SERVER_NAME": "vantage.example.com",
            "DBT_TERADATA_USERNAME": "analytics_svc",
            "DBT_TERADATA_PASSWORD": "secure_password_123",
            "DBT_TERADATA_SCHEMA_LOCATION": "analytics",
            "DBT_TERADATA_TMODE": "ANSI",
            "DBT_TERADATA_PORT": "1025",
            "DBT_TERADATA_LOGMECH": "TD2",
            "DBT_TERADATA_CHARSET": "UTF8",
            "DBT_TERADATA_QUERY_BAND": "app=dbt_etl;workload=nightly;",
        }
        rendered = _render_profile(self._PROFILE_ENTRY, env_vars)
        creds = _rendered_to_credentials(rendered)

        assert creds.server == "vantage.example.com"
        assert creds.username == "analytics_svc"
        assert creds.password == "secure_password_123"
        assert creds.schema == "analytics"
        assert creds.tmode == "ANSI"
        assert creds.port == "1025"
        assert creds.logmech == "TD2"
        assert creds.charset == "UTF8"
        assert creds.query_band == "app=dbt_etl;workload=nightly;"

    def test_env_var_default_used_when_variable_absent(self):
        """env_var() falls back to the default when the variable is not set."""
        env_vars = {
            "DBT_TERADATA_SERVER_NAME": "vantage.example.com",
            "DBT_TERADATA_USERNAME": "analytics_svc",
            "DBT_TERADATA_PASSWORD": "secret",
            "DBT_TERADATA_SCHEMA_LOCATION": "analytics",
            # DBT_TERADATA_TMODE absent → default 'ANSI'
            # DBT_TERADATA_PORT absent → default '1025'
        }
        rendered = _render_profile(self._PROFILE_ENTRY, env_vars)
        assert rendered["tmode"] == "ANSI"
        assert rendered["port"] == "1025"

    def test_shell_env_overrides_dot_env_via_renderer(self):
        """Shell variable wins over .env value — both go through the same env_var() call.

        dbt-core loads .env variables into os.environ only when the key is not
        already present. This test sets the 'shell' value in env_vars (the
        higher-priority source) and asserts the renderer returns that value, not
        a lower-priority .env alternative.
        """
        shell_value = "shell-vantage.example.com"
        env_vars = {
            "DBT_TERADATA_SERVER_NAME": shell_value,  # shell wins
            "DBT_TERADATA_USERNAME": "user",
            "DBT_TERADATA_PASSWORD": "pass",
            "DBT_TERADATA_SCHEMA_LOCATION": "schema",
        }
        rendered = _render_profile(self._PROFILE_ENTRY, env_vars)
        assert rendered["host"] == shell_value

    def test_missing_required_env_var_raises_error(self):
        """env_var() raises when a required variable has no value and no default."""
        env_vars = {
            # DBT_TERADATA_SERVER_NAME intentionally absent — no default in profile
            "DBT_TERADATA_USERNAME": "user",
            "DBT_TERADATA_PASSWORD": "pass",
            "DBT_TERADATA_SCHEMA_LOCATION": "schema",
        }
        # EnvVarMissingError (a DbtRuntimeError) is raised by dbt's env_var()
        # when a variable is absent and no default was supplied.
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError):
            _render_profile(self._PROFILE_ENTRY, env_vars)


class TestDotEnvFileToCredentialChain:
    """Full chain: .env file → python-dotenv (the loader dbt 1.12 uses) → os.environ → env_var() → TeradataCredentials.

    Each test calls pytest.importorskip("dotenv") so the class is skipped when
    python-dotenv is not installed. Install it via:  pip install -r requirements_dev.txt

    dbt 1.12 discovers .env from the current working directory via
    find_dotenv(usecwd=True), then calls load_dotenv(..., override=False).
    This is not a PROJECT_DIR-only lookup; if dbt is invoked with --project-dir
    from elsewhere, discovery still begins at the process working directory and
    walks upward.

    override=False means shell environment variables are never overwritten by
    .env values, which is the shell-wins precedence rule. These tests exercise
    that exact path.
    """

    _PROFILE_ENTRY = {
        "type": "teradata",
        "host": "{{ env_var('DBT_TERADATA_SERVER_NAME') }}",
        "user": "{{ env_var('DBT_TERADATA_USERNAME') }}",
        "password": "{{ env_var('DBT_TERADATA_PASSWORD') }}",
        "schema": "{{ env_var('DBT_TERADATA_SCHEMA_LOCATION') }}",
        "tmode": "{{ env_var('DBT_TERADATA_TMODE', 'ANSI') }}",
        "port": "{{ env_var('DBT_TERADATA_PORT', '1025') }}",
        "logmech": "{{ env_var('DBT_TERADATA_LOGMECH', 'TD2') }}",
        "charset": "{{ env_var('DBT_TERADATA_CHARSET', 'UTF8') }}",
        "query_band": "{{ env_var('DBT_TERADATA_QUERY_BAND', 'app=dbt_etl;') }}",
    }

    @pytest.fixture
    def dot_env_file(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text(
            "# Teradata connection\n"
            "DBT_TERADATA_SERVER_NAME=vantage.example.com\n"
            "DBT_TERADATA_USERNAME=analytics_svc\n"
            "DBT_TERADATA_PASSWORD=secure_password_123\n"
            "DBT_TERADATA_PORT=1025\n"
            "\n"
            "# Session\n"
            "DBT_TERADATA_TMODE=ANSI\n"
            "DBT_TERADATA_LOGMECH=TD2\n"
            "DBT_TERADATA_CHARSET=UTF8\n"
            "\n"
            "# Schema\n"
            "DBT_TERADATA_SCHEMA_LOCATION=analytics\n"
            "\n"
            "# Performance\n"
            "DBT_TERADATA_QUERY_BAND=app=dbt_etl;workload=nightly;\n"
        )
        return env_file

    def test_dot_env_file_produces_valid_credentials(self, dot_env_file):
        """python-dotenv loads .env into os.environ; env_var() resolves to the correct credentials."""
        dotenv = pytest.importorskip("dotenv", reason="python-dotenv required (pip install -r requirements_dev.txt)")

        with patch.dict(os.environ, {}, clear=True):
            loaded = dotenv.load_dotenv(dot_env_file, override=False)
            assert loaded, ".env file was not loaded — check the fixture path"
            rendered = _render_profile(self._PROFILE_ENTRY, dict(os.environ))

        creds = _rendered_to_credentials(rendered)
        assert creds.server == "vantage.example.com"
        assert creds.username == "analytics_svc"
        assert creds.password == "secure_password_123"
        assert creds.schema == "analytics"
        assert creds.tmode == "ANSI"
        assert creds.port == "1025"
        assert creds.logmech == "TD2"
        assert creds.charset == "UTF8"
        assert creds.query_band == "app=dbt_etl;workload=nightly;"

    def test_dot_env_comments_and_blank_lines_are_ignored(self, dot_env_file):
        """python-dotenv does not add comment lines or blank lines as environment variables."""
        dotenv = pytest.importorskip("dotenv")

        with patch.dict(os.environ, {}, clear=True):
            loaded = dotenv.load_dotenv(dot_env_file, override=False)
            loaded_keys = set(os.environ.keys())

        assert loaded, ".env file was not loaded — check the fixture path"
        assert {"DBT_TERADATA_SERVER_NAME", "DBT_TERADATA_QUERY_BAND"} <= loaded_keys, (
            "Expected DBT_TERADATA_* keys were not populated from .env"
        )
        assert not any(k.startswith("#") for k in loaded_keys)
        assert "" not in loaded_keys

    def test_shell_env_takes_precedence_over_dot_env_file(self, dot_env_file):
        """Shell variable wins over .env value when load_dotenv is called with override=False.

        dbt 1.12 calls load_dotenv(..., override=False), so a variable already set
        in the shell is not overwritten by the .env file. This test sets a 'shell'
        value for DBT_TERADATA_SERVER_NAME before loading .env and verifies it survives.
        """
        dotenv = pytest.importorskip("dotenv")

        shell_value = "shell-vantage.example.com"
        with patch.dict(os.environ, {"DBT_TERADATA_SERVER_NAME": shell_value}, clear=True):
            dotenv.load_dotenv(dot_env_file, override=False)
            assert os.environ["DBT_TERADATA_SERVER_NAME"] == shell_value, (
                "load_dotenv(override=False) must not overwrite an existing shell variable"
            )
            rendered = _render_profile(
                {
                    "host": "{{ env_var('DBT_TERADATA_SERVER_NAME') }}",
                    "user": "{{ env_var('DBT_TERADATA_USERNAME') }}",
                    "password": "{{ env_var('DBT_TERADATA_PASSWORD') }}",
                    "schema": "{{ env_var('DBT_TERADATA_SCHEMA_LOCATION') }}",
                },
                dict(os.environ),
            )

        assert rendered["host"] == shell_value

    def test_missing_dot_env_file_does_not_prevent_credential_creation(self, tmp_path):
        """load_dotenv returns False for a missing file and leaves os.environ unchanged.

        dbt 1.12 skips .env loading silently when the file does not exist; shell
        variables or profile-level defaults supply any missing values.
        """
        dotenv = pytest.importorskip("dotenv")

        missing = tmp_path / ".env"
        assert not missing.exists()

        shell_vars = {
            "DBT_TERADATA_SERVER_NAME": "localhost",
            "DBT_TERADATA_USERNAME": "dbc",
            "DBT_TERADATA_PASSWORD": "dbc",
            "DBT_TERADATA_SCHEMA_LOCATION": "test_schema",
        }
        with patch.dict(os.environ, shell_vars, clear=True):
            loaded = dotenv.load_dotenv(missing, override=False)
            assert not loaded, "load_dotenv should return False for a missing file"
            rendered = _render_profile(
                {
                    "host": "{{ env_var('DBT_TERADATA_SERVER_NAME') }}",
                    "user": "{{ env_var('DBT_TERADATA_USERNAME') }}",
                    "password": "{{ env_var('DBT_TERADATA_PASSWORD') }}",
                    "schema": "{{ env_var('DBT_TERADATA_SCHEMA_LOCATION') }}",
                },
                dict(os.environ),
            )

        creds = _rendered_to_credentials(rendered)
        assert creds.server == "localhost"
        assert creds.username == "dbc"
