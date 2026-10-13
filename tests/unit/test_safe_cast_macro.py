"""Unit tests for the teradata__safe_cast macro.

Pure unit tests (no database required). They render
dbt/include/teradata/macros/utils/safe_cast.sql with a lightweight stub for the
dbt-provided `dbt` global and assert that:

  1. An *unsized* VARCHAR / CHARACTER VARYING is widened to Teradata's maximum
     VARCHAR width. Teradata silently defaults a lengthless VARCHAR to
     VARCHAR(1) instead of rejecting it -- `TRYCAST('abc' AS VARCHAR)` returns
     'a' -- so without the widening this would truncate data without warning.
     An unsized type is also the spelling that actually reaches the macro for
     unit-test fixtures, because dbt-adapters' `format_row` macro strips the
     declared length from string fixture values on purpose (dbt-core GH-11974).
  2. A length the caller *did* specify is passed through untouched, so an
     explicit safe_cast(x, 'varchar(10)') is never silently widened. TRYCAST
     applies the declared width itself, truncating per normal CAST semantics.
     This includes the sized `character varying(n)` / `character(n)` spellings
     that TeradataColumn.data_type reports, which are valid Teradata types and
     need no special handling.
  3. Every other type is passed through untouched.
"""

import os

import pytest
from jinja2 import Environment

from dbt.adapters.teradata.column import TeradataColumn


_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_SAFE_CAST = os.path.join(
    _REPO_ROOT, "dbt", "include", "teradata", "macros", "utils", "safe_cast.sql"
)


class _DbtStub:
    """Stands in for the `dbt` global; only string_literal is used here."""

    @staticmethod
    def string_literal(value):
        return "'{}'".format(value)


@pytest.fixture(scope="module")
def safe_cast():
    with open(_SAFE_CAST, "r", encoding="utf-8") as fh:
        source = fh.read()

    env = Environment()
    template = env.from_string(source)
    module = template.make_module({"dbt": _DbtStub()})
    return lambda field, type_: str(module.teradata__safe_cast(field, type_)).strip()


@pytest.mark.parametrize(
    "declared_type",
    [
        "varchar",
        "VARCHAR",
        "character varying",
        "CHARACTER VARYING",
        "  character varying  ",
    ],
)
def test_unsized_varchar_is_widened(safe_cast, declared_type):
    assert safe_cast("my_column", declared_type) == "trycast(my_column as varchar(64000))"


@pytest.mark.parametrize(
    "declared_type",
    [
        "varchar(10)",
        "VARCHAR(10)",
        "character varying(10)",
        "CHARACTER VARYING(10)",
    ],
)
def test_explicit_length_is_preserved(safe_cast, declared_type):
    """An explicitly requested length must never be silently widened."""
    assert safe_cast("my_column", declared_type) == f"trycast(my_column as {declared_type})"


@pytest.mark.parametrize(
    "declared_type",
    ["integer", "date", "timestamp(6)", "decimal(18,2)", "character(5)", "long varchar"],
)
def test_other_types_pass_through(safe_cast, declared_type):
    assert safe_cast("my_column", declared_type) == f"trycast(my_column as {declared_type})"


def test_numeric_field_is_quoted(safe_cast):
    """Numeric literals are wrapped via dbt.string_literal before casting."""
    assert safe_cast(123, "varchar(10)") == "trycast('123' as varchar(10))"


@pytest.mark.parametrize(
    "dtype, char_size, reported_type",
    [
        ("varchar", 256, "character varying(256)"),
        ("varchar", 64000, "character varying(64000)"),
        ("character", 10, "character(10)"),
    ],
)
def test_adapter_reported_sized_types_pass_through(safe_cast, dtype, char_size, reported_type):
    """Types exactly as TeradataColumn.data_type reports them survive untouched.

    This closes the loop between the two halves of the contract: the string the
    adapter hands out for a sized character column, and what the macro does with
    it. Both sized spellings are valid Teradata and need no special handling --
    verified against Teradata 20.00.32.12:

        TRYCAST('abc' AS character varying(256)) -> 'abc'
        TRYCAST('abc' AS character(10))          -> 'abc       '

    Only the *unsized* spellings are dangerous (they collapse to VARCHAR(1)),
    and those are covered by test_unsized_varchar_is_widened above.
    """
    column = TeradataColumn(
        column="my_column",
        dtype=dtype,
        char_size=char_size,
        numeric_precision=None,
        numeric_scale=None,
    )
    assert column.data_type == reported_type
    assert safe_cast("my_column", column.data_type) == (
        f"trycast(my_column as {reported_type})"
    )
