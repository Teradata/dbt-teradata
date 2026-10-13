"""Unit coverage for `TeradataConnectionManager.exception_handler` classification.

`exception_handler` decides which exceptions reach the caller unchanged and
which get normalised into `DbtRuntimeError`. That classification is load-bearing
but was previously untested, so this module pins it.

Why the handler checks `DbtBaseException` rather than `DbtRuntimeError`:
`execute_macro` wraps the *entire* macro render in `exception_handler`
(`dbt/adapters/base/impl.py`, `with self.connections.exception_handler(...)`),
not just the `execute()` calls inside it. So a `{{ adapter.some_method() }}`
call in a macro body -- for example `{{ adapter.verify_database(...) }}` in
`teradata__drop_schema` -- raises *through* this handler. `verify_database`
raises `dbt_common.exceptions.NotImplementedError`, which derives from
`DbtBaseException` without going through `DbtRuntimeError`; with the narrower
check it was rewrapped and callers could no longer tell a deliberate guard from
an arbitrary driver failure.
"""

import dbt_common.exceptions
import pytest
import teradatasql

from dbt.adapters.teradata.connections import TeradataConnectionManager


class _StubConnectionManager(TeradataConnectionManager):
    """Exercise `exception_handler` without opening a real connection."""

    def __init__(self):  # noqa: D107 - deliberately skips the real __init__
        self.rollback_calls = 0

    def rollback_if_open(self):
        self.rollback_calls += 1


@pytest.fixture
def manager():
    return _StubConnectionManager()


# Every dbt-raised exception must reach the caller with its type intact.
@pytest.mark.parametrize(
    "exc",
    [
        # Raised by verify_database's cross-database guard. Not a
        # DbtRuntimeError subclass, so it is the case that motivated widening
        # the check to DbtBaseException.
        dbt_common.exceptions.NotImplementedError("cross-db"),
        # "This is a bug, please report it" -- also a direct DbtBaseException
        # subclass. Flattening it to DbtRuntimeError would discard that signal,
        # so it is deliberately passed through as well.
        dbt_common.exceptions.DbtInternalError("internal"),
        # Already covered before the widening; pinned to show it is unchanged.
        dbt_common.exceptions.DbtRuntimeError("runtime"),
        dbt_common.exceptions.CompilationError("compilation"),
    ],
    ids=["not_implemented", "internal", "runtime", "compilation"],
)
def test_dbt_exceptions_pass_through_unmodified(manager, exc):
    with pytest.raises(type(exc)) as raised:
        with manager.exception_handler("select 1"):
            raise exc

    assert raised.value is exc
    assert manager.rollback_calls == 1


def test_non_dbt_exception_is_normalised_to_runtime_error(manager):
    original = ValueError("driver blew up")

    with pytest.raises(dbt_common.exceptions.DbtRuntimeError) as raised:
        with manager.exception_handler("select 1"):
            raise original

    assert raised.value.__cause__ is original
    assert manager.rollback_calls == 1


def test_driver_error_becomes_dbt_database_error(manager):
    original = teradatasql.DatabaseError("  [Error 3802] Database does not exist  ")

    with pytest.raises(dbt_common.exceptions.DbtDatabaseError) as raised:
        with manager.exception_handler("select 1"):
            raise original

    assert raised.value.__cause__ is original
    # The message is stripped, and the driver error never reaches the generic
    # `except Exception` branch, so no rollback_if_open() happens there.
    assert "[Error 3802]" in str(raised.value)


def test_no_exception_leaves_connection_untouched(manager):
    with manager.exception_handler("select 1"):
        pass

    assert manager.rollback_calls == 0
