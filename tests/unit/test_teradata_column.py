"""Unit tests for IDE-26634: TeradataColumn CHAR vs VARCHAR type rendering.

The `valid_history` incremental strategy (teradata__get_incremental_valid_history_sql
in dbt/include/teradata/macros/materializations/incremental/strategies.sql) feeds
`column.data_type` straight into a `TD_NORMALIZE_MEET ... RETURNS (...)` clause. If a
column is physically fixed-width CHAR(n), that clause must declare it as CHAR(n) too,
or Teradata rejects it with [Error 9134] Normalize: The output grouping column list
does not match with input grouping column list.

Root cause was twofold:
  1. teradata__get_columns_in_relation (adapters.sql) collapsed both the CF (CHAR)
     and CV (VARCHAR) catalog codes into the same 'CHARACTER' dtype label.
  2. The base Column.data_type property always renders string columns via the
     classmethod Column.string_type(), which unconditionally returns
     "character varying(n)" regardless of the column's true physical type.

Fix: TeradataColumn overrides the `data_type` property (not `string_type()`) to
special-case dtype == "character". `string_type()` itself is left untouched as
the inherited classmethod, because dbt-core's SQLAdapter.expand_column_types()
calls it unbound as `self.Column.string_type(size)` -- overriding it as an
instance method would break that schema-expansion call path.

These tests exercise TeradataColumn directly (dtype -> data_type), independent of a
live database connection.
"""

from dbt.adapters.base.column import Column
from dbt.adapters.teradata.column import TeradataColumn


def _make_column(dtype, char_size=None):
    return TeradataColumn(column="marketable_flag", dtype=dtype, char_size=char_size)


class TestTeradataColumnDataType:
    def test_char_dtype_renders_fixed_width_character(self):
        column = _make_column("character", char_size=1)
        assert column.is_string() is True
        assert column.data_type == "character(1)"

    def test_varchar_dtype_renders_character_varying(self):
        column = _make_column("character varying", char_size=10)
        assert column.is_string() is True
        assert column.data_type == "character varying(10)"

    def test_char_and_varchar_are_distinguishable(self):
        """The two dtypes must not collapse into the same data_type string."""
        char_column = _make_column("character", char_size=1)
        varchar_column = _make_column("character varying", char_size=1)

        assert char_column.data_type != varchar_column.data_type
        assert char_column.data_type == "character(1)"
        assert varchar_column.data_type == "character varying(1)"

    def test_char_dtype_case_insensitive(self):
        column = _make_column("CHARACTER", char_size=5)
        assert column.data_type == "character(5)"


class TestTeradataColumnStringTypeClassmethodApi:
    """string_type() must remain callable unbound, on the class, exactly like
    the base Column.string_type(). dbt-core's SQLAdapter.expand_column_types()
    calls it this way (`self.Column.string_type(col_string_size)`) when
    widening a string column during on_schema_change/schema-expansion. If
    string_type() were overridden as an instance method, that call would bind
    the size argument to `self` and raise a TypeError.
    """

    def test_string_type_is_inherited_unmodified_classmethod(self):
        # Bound classmethods produce distinct wrapper objects per subclass, so
        # compare the underlying function to confirm TeradataColumn does not
        # provide its own override.
        assert TeradataColumn.string_type.__func__ is Column.string_type.__func__

    def test_string_type_callable_unbound_on_the_class(self):
        # Mirrors dbt-core's `self.Column.string_type(col_string_size)` call.
        assert TeradataColumn.string_type(20) == "character varying(20)"
