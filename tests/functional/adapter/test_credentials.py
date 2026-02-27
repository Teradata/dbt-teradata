import pytest
from unittest.mock import patch

import dbt_common.exceptions

from dbt.adapters.teradata.connections import TeradataCredentials


def _make_credentials(**overrides):
    """Helper to build a TeradataCredentials with sensible defaults.

    By default produces a valid TD2 credential set.  Pass keyword
    arguments to override any field.
    """
    defaults = {
        "server": "localhost",
        "schema": "test_schema",
        "username": "test_user",
        "password": "test_pass",
    }
    defaults.update(overrides)
    return TeradataCredentials(**defaults)


# ── TD2 (default logmech) ───────────────────────────────────────────


class TestTD2Logmech:
    """TD2 is the default authentication mechanism and requires user + password."""

    def test_td2_explicit_valid(self):
        creds = _make_credentials(logmech="TD2")
        assert creds.username == "test_user"
        assert creds.password == "test_pass"
        assert creds.logmech == "TD2"

    def test_td2_implicit_valid(self):
        """logmech=None defaults to TD2 behaviour."""
        creds = _make_credentials(logmech=None)
        assert creds.username == "test_user"
        assert creds.password == "test_pass"

    def test_td2_explicit_missing_username(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="user.*parameter"):
            _make_credentials(logmech="TD2", username=None)

    def test_td2_explicit_missing_password(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="password.*parameter"):
            _make_credentials(logmech="TD2", password=None)

    def test_td2_explicit_missing_both(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="user.*parameter"):
            _make_credentials(logmech="TD2", username=None, password=None)

    def test_td2_implicit_missing_username(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="user.*parameter"):
            _make_credentials(logmech=None, username=None)

    def test_td2_implicit_missing_password(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="password.*parameter"):
            _make_credentials(logmech=None, password=None)

    def test_td2_case_insensitive(self):
        """TD2 check should be case-insensitive."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="user.*parameter"):
            _make_credentials(logmech="td2", username=None)

        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="user.*parameter"):
            _make_credentials(logmech="Td2", username=None)


# ── BROWSER logmech ─────────────────────────────────────────────────


class TestBrowserLogmech:
    """BROWSER auth must not have user or password specified."""

    def test_browser_valid_no_credentials(self):
        creds = _make_credentials(logmech="BROWSER", username=None, password=None)
        assert creds.logmech == "BROWSER"
        assert creds.username is None
        assert creds.password is None

    def test_browser_rejects_username(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="BROWSER"):
            _make_credentials(logmech="BROWSER", username="user", password=None)

    def test_browser_rejects_password(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="BROWSER"):
            _make_credentials(logmech="BROWSER", username=None, password="pass")

    def test_browser_rejects_both(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="BROWSER"):
            _make_credentials(logmech="BROWSER", username="user", password="pass")

    def test_browser_case_insensitive(self):
        creds = _make_credentials(logmech="browser", username=None, password=None)
        assert creds.logmech == "browser"

        creds = _make_credentials(logmech="Browser", username=None, password=None)
        assert creds.logmech == "Browser"


# ── KRB5 logmech (the fix target) ───────────────────────────────────


class TestKRB5Logmech:
    """KRB5 (Kerberos) should NOT require user/password."""

    def test_krb5_no_credentials(self):
        creds = _make_credentials(logmech="KRB5", username=None, password=None)
        assert creds.logmech == "KRB5"
        assert creds.username is None
        assert creds.password is None

    def test_krb5_with_username_only(self):
        creds = _make_credentials(logmech="KRB5", username="kerb_user", password=None)
        assert creds.username == "kerb_user"

    def test_krb5_with_password_only(self):
        creds = _make_credentials(logmech="KRB5", username=None, password="kerb_pass")
        assert creds.password == "kerb_pass"

    def test_krb5_with_both_credentials(self):
        creds = _make_credentials(logmech="KRB5", username="user", password="pass")
        assert creds.username == "user"
        assert creds.password == "pass"

    def test_krb5_case_insensitive(self):
        creds = _make_credentials(logmech="krb5", username=None, password=None)
        assert creds.logmech == "krb5"


# ── LDAP logmech ─────────────────────────────────────────────────────


class TestLDAPLogmech:
    """LDAP should not enforce user/password at the adapter level."""

    def test_ldap_no_credentials(self):
        creds = _make_credentials(logmech="LDAP", username=None, password=None)
        assert creds.logmech == "LDAP"

    def test_ldap_with_credentials(self):
        creds = _make_credentials(logmech="LDAP", username="ldap_user", password="ldap_pass")
        assert creds.username == "ldap_user"
        assert creds.password == "ldap_pass"


# ── TDNEGO logmech ───────────────────────────────────────────────────


class TestTDNEGOLogmech:
    """TDNEGO should not enforce user/password at the adapter level."""

    def test_tdnego_no_credentials(self):
        creds = _make_credentials(logmech="TDNEGO", username=None, password=None)
        assert creds.logmech == "TDNEGO"

    def test_tdnego_with_credentials(self):
        creds = _make_credentials(logmech="TDNEGO", username="user", password="pass")
        assert creds.username == "user"


# ── JWT logmech ──────────────────────────────────────────────────────


class TestJWTLogmech:
    """JWT should not enforce user/password at the adapter level."""

    def test_jwt_no_credentials(self):
        creds = _make_credentials(logmech="JWT", username=None, password=None)
        assert creds.logmech == "JWT"

    def test_jwt_with_credentials(self):
        creds = _make_credentials(logmech="JWT", username="user", password="pass")
        assert creds.username == "user"


# ── Schema validation ────────────────────────────────────────────────


class TestSchemaValidation:
    """Schema is always required, regardless of logmech."""

    def test_missing_schema_td2(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="schema.*parameter"):
            _make_credentials(schema=None)

    def test_missing_schema_browser(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="schema.*parameter"):
            _make_credentials(logmech="BROWSER", username=None, password=None, schema=None)

    def test_missing_schema_krb5(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="schema.*parameter"):
            _make_credentials(logmech="KRB5", username=None, password=None, schema=None)


# ── Database / schema equivalence ────────────────────────────────────


class TestDatabaseSchemaEquivalence:
    """Teradata treats database and schema as the same thing."""

    def test_database_none_is_valid(self):
        creds = _make_credentials(database=None)
        assert creds.schema == "test_schema"

    def test_database_matches_schema(self):
        creds = _make_credentials(database="test_schema", schema="test_schema")
        assert creds.database == "test_schema"

    def test_database_differs_from_schema(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="does not match"):
            _make_credentials(database="other_db", schema="test_schema")


# ── TERA transaction mode ────────────────────────────────────────────


class TestTERAMode:
    """TERA mode should log an informational message but not raise."""

    def test_tera_mode_logs_warning(self):
        with patch("dbt.adapters.teradata.connections.logger") as mock_logger:
            creds = _make_credentials(tmode="TERA")
            assert creds.tmode == "TERA"
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args[0][0]
            assert "TERA transaction mode" in call_args

    def test_ansi_mode_no_warning(self):
        with patch("dbt.adapters.teradata.connections.logger") as mock_logger:
            creds = _make_credentials(tmode="ANSI")
            assert creds.tmode == "ANSI"
            mock_logger.info.assert_not_called()


# ── Unknown / arbitrary logmech ──────────────────────────────────────


class TestUnknownLogmech:
    """An unrecognised logmech value should pass through without error."""

    def test_unknown_logmech_no_credentials(self):
        creds = _make_credentials(logmech="CUSTOM_MECH", username=None, password=None)
        assert creds.logmech == "CUSTOM_MECH"

    def test_unknown_logmech_with_credentials(self):
        creds = _make_credentials(logmech="CUSTOM_MECH", username="u", password="p")
        assert creds.username == "u"


# ── Empty-string edge cases ──────────────────────────────────────────


class TestEmptyStringCredentials:
    """Empty strings are not None — they bypass the null checks."""

    def test_td2_empty_username_accepted(self):
        """Empty string username is not None, so no error is raised."""
        creds = _make_credentials(logmech="TD2", username="", password="pass")
        assert creds.username == ""

    def test_td2_empty_password_accepted(self):
        """Empty string password is not None, so no error is raised."""
        creds = _make_credentials(logmech="TD2", username="user", password="")
        assert creds.password == ""

    def test_browser_empty_username_rejected(self):
        """Empty string is not None, so BROWSER rejects it."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="BROWSER"):
            _make_credentials(logmech="BROWSER", username="", password=None)

    def test_browser_empty_password_rejected(self):
        """Empty string is not None, so BROWSER rejects it."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="BROWSER"):
            _make_credentials(logmech="BROWSER", username=None, password="")


# ── Error precedence ─────────────────────────────────────────────────


class TestErrorPrecedence:
    """Validate which error fires first when multiple fields are invalid."""

    def test_td2_missing_user_before_missing_schema(self):
        """Username check fires before schema check for TD2."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="user.*parameter"):
            _make_credentials(logmech="TD2", username=None, schema=None)

    def test_krb5_missing_schema_only(self):
        """KRB5 skips user/password check, so schema error is the first error."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="schema.*parameter"):
            _make_credentials(logmech="KRB5", username=None, password=None, schema=None)

    def test_schema_missing_for_ldap(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="schema.*parameter"):
            _make_credentials(logmech="LDAP", username=None, password=None, schema=None)

    def test_schema_missing_for_tdnego(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="schema.*parameter"):
            _make_credentials(logmech="TDNEGO", username=None, password=None, schema=None)

    def test_schema_missing_for_jwt(self):
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="schema.*parameter"):
            _make_credentials(logmech="JWT", username=None, password=None, schema=None)
