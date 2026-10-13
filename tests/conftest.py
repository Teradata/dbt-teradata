import pytest
import os
import random
import re
import teradatasql
from datetime import datetime

from tests.teradata_cleanup import run_sql_ignoring_missing_database
#from dotenv import load_dotenv, find_dotenv
#from pathlib import Path

#load_dotenv("../test.env")

# Environment variables read by the test suite:
#
#   Connection (with defaults for Vantage Express running on localhost):
#     DBT_TERADATA_SERVER_NAME  -- hostname           (default: 'localhost')
#     DBT_TERADATA_USERNAME     -- username           (default: 'dbc')
#     DBT_TERADATA_PASSWORD     -- password           (default: 'dbc')
#     DBT_TERADATA_TMODE        -- transaction mode   (default: 'ANSI')
#
#   Grants tests (created automatically if set):
#     DBT_TEST_USER_1
#     DBT_TEST_USER_2
#     DBT_TEST_USER_3
#     DBT_TEST_ROLE_1           -- role used by the role-aware grants tests
#                                  (teradata_grants_extended)
#
#   OTF / DATALAKE tests (tests/functional/adapter/test_otf_integration.py).
#   When BOTH are set, OTF integration tests run; otherwise they are skipped:
#     DBT_TERADATA_DATALAKE       -- pre-created DATALAKE object name
#     DBT_TERADATA_OTF_DATABASE   -- pre-created OTF database within the DATALAKE

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be locals
pytest_plugins = ["dbt.tests.fixtures.project"]


# Teradata identifiers: start with a letter, then letters/digits/_/$/#, max 128 chars.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,127}$")


def _checked_identifier(env_var):
    """Return the identifier in `env_var`, or None if it is unset/empty.

    The value is interpolated straight into CREATE USER / CREATE ROLE DDL, so
    reject anything that is not a plain Teradata identifier up front. A typo or
    a quoted/spaced value otherwise surfaces as a confusing syntax error from
    the database instead of pointing at the misconfigured environment variable.
    """
    name = os.getenv(env_var)
    if not name:
        return None
    name = name.strip()
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"{env_var}={name!r} is not a valid Teradata identifier. Use a plain, "
            "unquoted name: a letter followed by letters, digits, '_', '$' or '#' "
            "(max 128 characters)."
        )
    return name


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    hostname='localhost'
    username='dbc'
    password='dbc'
    tmode='ANSI'

    if os.getenv('DBT_TERADATA_SERVER_NAME'):
        hostname=os.getenv('DBT_TERADATA_SERVER_NAME')

    if os.getenv('DBT_TERADATA_USERNAME'):
        username=os.getenv('DBT_TERADATA_USERNAME')

    if os.getenv('DBT_TERADATA_PASSWORD'):
        password=os.getenv('DBT_TERADATA_PASSWORD')

    if os.getenv('DBT_TERADATA_TMODE'):
        tmode=os.getenv('DBT_TERADATA_TMODE')


    with teradatasql.connect (host=hostname,user=username,password=password) as con:
        with con.cursor () as cur:
            TEST_USER_ENV_VARS = ["DBT_TEST_USER_1", "DBT_TEST_USER_2", "DBT_TEST_USER_3"]
            for env_var in TEST_USER_ENV_VARS:
                user_name = _checked_identifier(env_var)
                if user_name:
                    try:
                        cur.execute ("create user {} from dbc as permanent=100000000 BYTES,password={};".format(user_name,user_name))
                    except Exception as ex:
                        if "[Error 5612]" in str (ex):
                            print ("Ignoring", str (ex).split ("\n") [0])
                        else:
                            raise # rethrow

            # Create role for role-aware grants tests if DBT_TEST_ROLE_1 is set
            role_name = _checked_identifier('DBT_TEST_ROLE_1')
            if role_name:
                try:
                    cur.execute('CREATE ROLE {}'.format(role_name))
                except Exception as ex:
                    if '[Error 5612]' in str(ex) or '[Error 3802]' in str(ex) or 'already exists' in str(ex).lower():
                        print('Ignoring', str(ex).split('\n')[0])
                    else:
                        raise

    return {
        'type': 'teradata',
        'threads': 4,
        'host': hostname, #{{os.getenv('HOST'), 'localhost'}},
        'user': username, #{{os.getenv('USER'), 'dbc'}},
        'password': password, #{{os.getenv('PASSWORD'), 'dbc'}},
        'tmode': tmode,
        'log': '0'
    }

@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    unique_schema = f"dbt_test_{prefix}"
    return unique_schema


def _drop_alternate_test_schemas(project):
    """Drop the test schemas that teradata__drop_schema deliberately refuses to.

    Teradata equates a database with a schema, so `teradata__drop_schema` guards
    every drop with `adapter.verify_database()` and refuses to drop anything
    other than the database configured in the profile -- DROP DATABASE is
    destructive and must never be aimed at a database dbt was not pointed at.

    A handful of upstream base tests (e.g. BaseGenerateProject and
    BaseDocsGenReferences) call `project.create_test_schema()` for a second,
    alternate schema. That registers it in `created_schemas`, so the upstream
    `project` fixture teardown later drops it through the guarded macro and
    trips the guard. Drop those alternate schemas here instead, directly via
    raw SQL -- an explicit, intentional exception to the guard confined to test
    teardown -- and de-register them so the upstream teardown only ever sees
    the one schema the guard will actually accept.
    """
    # Keep back exactly the schema `verify_database` will accept, and raw-drop
    # every other one. The guard compares against
    # `adapter.config.credentials.schema`, and `project.adapter` is whichever
    # adapter the *last* dbt command created -- so for the multi-target clone
    # suites, which run `dbt clone --target otherschema`, it is the alternate
    # "<schema>_other" and not `project.test_schema`. Comparing against the
    # same value the guard uses keeps the two in step whichever target was
    # last active; `project.test_schema` is only a fallback for when no adapter
    # is registered at all.
    #
    # `project.adapter` looks the adapter up in the adapter factory, and the
    # `debug`, `deps` and `clean` commands never install one -- see
    # dbt-labs/dbt-core#5041. Scope the except to that lookup alone: an
    # unexpected `.config`/`.credentials` shape on an adapter that *is*
    # registered is a real bug and must surface rather than silently fall back
    # to the wrong schema.
    try:
        adapter = project.adapter
    except (KeyError, AttributeError):
        configured_schema = (project.test_schema or "").casefold()
    else:
        configured_schema = (adapter.config.credentials.schema or "").casefold()
    remaining = []

    for schema_name in project.created_schemas:
        if schema_name.casefold() == configured_schema:
            remaining.append(schema_name)
            continue
        for statement in (
            f"DELETE DATABASE {schema_name} ALL;",
            f"DROP DATABASE {schema_name};",
        ):
            try:
                # Swallows only "database does not exist"; anything else means
                # the schema is being leaked on the server and is re-raised.
                run_sql_ignoring_missing_database(project, statement)
            except (KeyError, AttributeError):
                # `project.run_sql` reaches the server through
                # `project.adapter`, which looks the adapter up in the adapter
                # factory. The `debug`, `deps` and `clean` commands never
                # install one, so the lookup raises KeyError (and `debug`
                # additionally raises AttributeError, because `Profile` has no
                # `load_dependencies`) -- see dbt-labs/dbt-core#5041. With no
                # adapter there is no connection to issue DROP DATABASE over,
                # so there is nothing this teardown can do. The upstream
                # `project` fixture swallows exactly these two exceptions a
                # moment later for the same reason; leave the schemas to it
                # rather than failing the test in teardown.
                return

    project.created_schemas = remaining


@pytest.fixture(scope="class")
def project(project):
    """Extend the upstream `project` fixture with Teradata-aware teardown.

    Requesting the same-named fixture from the dbt.tests.fixtures.project
    plugin means this runs as a wrapper: its teardown executes before the
    upstream one, which is exactly when the alternate schemas need to be
    cleared out.
    """
    yield project
    _drop_alternate_test_schemas(project)


@pytest.fixture(scope="class")
def prefix():
    # create a directory name that will be unique per test session
    _randint = random.randint(0, 9999)
    _runtime_timedelta = datetime.now() - datetime(1970, 1, 1, 0, 0, 0)
    _runtime = (int(_runtime_timedelta.total_seconds() * 1e6)) + _runtime_timedelta.microseconds
    prefix = f"{_runtime}{_randint:04}"
    return prefix
