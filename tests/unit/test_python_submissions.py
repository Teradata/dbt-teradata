"""Unit tests for client-side Python model support (IDE-24196).

These tests avoid a live Teradata instance by mocking the database-interaction
methods (_connect / _load_relation / _write_dataframe). They cover:

- adapter submission-method wiring
- pandas dtype -> Teradata type mapping (every kind)
- CREATE TABLE column DDL generation
- DataFrame -> row-binding conversion (NaN/NaT/numpy coercion)
- the submit() exec flow (ref resolution, model execution, write-back)
- error handling (bad return type, model raising, drop-if-exists 3807)
- the injected session shim
"""

import datetime

import numpy as np
import pandas as pd
import pytest

import dbt_common.exceptions
from dbt.adapters.teradata.impl import TeradataAdapter
from dbt.adapters.teradata.python_submissions import (
    DEFAULT_VARCHAR_LENGTH,
    TeradataClientPythonJobHelper,
    _TeradataSession,
    _as_bool,
    build_column_ddl,
    dataframe_to_bindings,
    pandas_dtype_to_teradata,
)


# --------------------------------------------------------------------------- #
# Adapter wiring
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "value, expected",
    [
        (True, True),
        (False, False),
        ("true", True),
        ("True", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        ("false", False),
        ("False", False),
        ("0", False),
        ("no", False),
        ("", False),
        (1, True),
        (0, False),
    ],
)
def test_as_bool_coerces_config_values(value, expected):
    assert _as_bool(value) is expected


@pytest.mark.parametrize("bad", [0, -1, "abc", None])
def test_batch_size_invalid_raises_clear_error(bad):
    parsed_model = {"alias": "m", "config": {"batch_size": bad}}
    helper = TeradataClientPythonJobHelper.__new__(TeradataClientPythonJobHelper)
    with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="batch_size"):
        TeradataClientPythonJobHelper.__init__(helper, parsed_model, None)


def test_batch_size_valid_and_default():
    helper = TeradataClientPythonJobHelper.__new__(TeradataClientPythonJobHelper)
    TeradataClientPythonJobHelper.__init__(
        helper, {"alias": "m", "config": {"batch_size": "500"}}, None
    )
    assert helper.batch_size == 500


def test_use_fastload_config_string_false_is_disabled():
    parsed_model = {"alias": "m", "config": {"use_fastload": "false"}}
    helper = TeradataClientPythonJobHelper.__new__(TeradataClientPythonJobHelper)
    # exercise the real __init__ config parsing (no DB connection needed)
    TeradataClientPythonJobHelper.__init__(helper, parsed_model, None)
    assert helper.use_fastload is False


def test_default_submission_method_is_client():
    assert TeradataAdapter.default_python_submission_method.fget(None) == "client"


def test_submission_helpers_maps_client():
    helpers = TeradataAdapter.python_submission_helpers.fget(None)
    assert helpers == {"client": TeradataClientPythonJobHelper}


# --------------------------------------------------------------------------- #
# Type mapping
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "series, expected",
    [
        (pd.Series([1, 2, 3], dtype="int64"), "BIGINT"),
        (pd.Series([1, 2, 3], dtype="int32"), "BIGINT"),
        (pd.Series([1, 2, 3], dtype="uint8"), "BIGINT"),
        (pd.Series([1.0, 2.5], dtype="float64"), "FLOAT"),
        (pd.Series([1.0, 2.5], dtype="float32"), "FLOAT"),
        (pd.Series([True, False], dtype="bool"), "BYTEINT"),
        (pd.Series(pd.to_datetime(["2024-01-01", "2024-02-01"])), "TIMESTAMP"),
    ],
)
def test_dtype_mapping_scalars(series, expected):
    assert pandas_dtype_to_teradata(series) == expected


def test_dtype_mapping_timedelta():
    s = pd.Series(pd.to_timedelta(["1 days", "2 days"]))
    assert pandas_dtype_to_teradata(s) == "INTERVAL DAY(4) TO SECOND"


def test_dtype_mapping_object_sizes_to_longest_value():
    s = pd.Series(["a", "abcdef", "abc"])
    assert pandas_dtype_to_teradata(s) == "VARCHAR(6) CHARACTER SET UNICODE"


def test_dtype_mapping_all_null_object_uses_default_length():
    s = pd.Series([None, None], dtype="object")
    assert pandas_dtype_to_teradata(s) == (
        f"VARCHAR({DEFAULT_VARCHAR_LENGTH}) CHARACTER SET UNICODE"
    )


def test_dtype_mapping_object_min_length_one():
    s = pd.Series([""], dtype="object")
    assert pandas_dtype_to_teradata(s) == "VARCHAR(1) CHARACTER SET UNICODE"


# --------------------------------------------------------------------------- #
# Column DDL
# --------------------------------------------------------------------------- #
def test_build_column_ddl():
    df = pd.DataFrame(
        {
            "id": pd.Series([1, 2], dtype="int64"),
            "amount": pd.Series([1.5, 2.5], dtype="float64"),
            "name": pd.Series(["ab", "abc"], dtype="object"),
        }
    )
    ddl = build_column_ddl(df)
    assert '"id" BIGINT' in ddl
    assert '"amount" FLOAT' in ddl
    assert '"name" VARCHAR(3) CHARACTER SET UNICODE' in ddl
    # columns comma-separated
    assert ddl.count(",") == 2


def test_build_column_ddl_escapes_embedded_quotes():
    # a column name containing a double quote must be escaped by doubling it,
    # not emitted raw (which would produce invalid DDL / allow identifier shaping).
    df = pd.DataFrame({'a"b': pd.Series([1], dtype="int64")})
    ddl = build_column_ddl(df)
    assert '"a""b" BIGINT' in ddl


# --------------------------------------------------------------------------- #
# Bindings
# --------------------------------------------------------------------------- #
def test_dataframe_to_bindings_coerces_numpy_and_nan():
    df = pd.DataFrame(
        {
            "id": pd.Series([1, 2], dtype="int64"),
            "amount": pd.Series([1.5, np.nan], dtype="float64"),
            "name": pd.Series(["x", None], dtype="object"),
        }
    )
    bindings = dataframe_to_bindings(df)
    assert len(bindings) == 2
    # numpy int64 -> python int
    assert bindings[0][0] == 1 and isinstance(bindings[0][0], int)
    assert bindings[0][1] == 1.5 and isinstance(bindings[0][1], float)
    assert bindings[0][2] == "x"
    # NaN -> None, None -> None
    assert bindings[1][1] is None
    assert bindings[1][2] is None


def test_dataframe_to_bindings_empty():
    df = pd.DataFrame({"id": pd.Series([], dtype="int64")})
    assert dataframe_to_bindings(df) == []


# --------------------------------------------------------------------------- #
# submit() exec flow
# --------------------------------------------------------------------------- #
COMPILED_TEMPLATE = '''\
def model(dbt, session):
    return dbt.ref("stg_orders")

def ref(*args, **kwargs):
    refs = {"stg_orders": "\\"myschema\\".\\"stg_orders\\""}
    key = ".".join(args)
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])

def source(*args, dbt_load_df_function):
    sources = {}
    key = ".".join(args)
    return dbt_load_df_function(sources[key])

config_dict = {}

class config:
    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    database = "myschema"
    schema = "myschema"
    identifier = "py_model"
    def __repr__(self):
        return '"myschema"."py_model"'

class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *a: source(*a, dbt_load_df_function=load_df_function)
        self.ref = lambda *a, **k: ref(*a, **k, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# --- Autogenerated dbt materialization code (dbt-teradata client-side) --- #
dbt = dbtObj(load_df_function)
df = model(dbt, session)
teradata__write_df(df, "\\"myschema\\".\\"py_model\\"")
'''


def _make_raw_helper():
    """Helper with real methods (only DB connection gets mocked per-test)."""
    helper = TeradataClientPythonJobHelper.__new__(TeradataClientPythonJobHelper)
    helper.parsed_model = {"alias": "py_model", "config": {}}
    helper.credentials = None
    helper.identifier = "py_model"
    helper.batch_size = 2536
    helper.use_fastload = False
    return helper


def _make_helper(captured, load_result=None):
    helper = TeradataClientPythonJobHelper.__new__(TeradataClientPythonJobHelper)
    helper.parsed_model = {"alias": "py_model", "config": {}}
    helper.credentials = None
    helper.identifier = "py_model"
    helper.batch_size = 2536
    helper.use_fastload = False

    def fake_load(relation_name):
        captured["loaded"] = relation_name
        return load_result if load_result is not None else pd.DataFrame({"a": [1]})

    def fake_write(df, target):
        captured["written_df"] = df
        captured["written_target"] = target

    helper._load_relation = fake_load
    helper._write_dataframe = fake_write
    return helper


def test_submit_runs_model_resolves_ref_and_writes():
    captured = {}
    src = pd.DataFrame({"order_id": [1, 2], "amount": [10.0, 20.0]})
    helper = _make_helper(captured, load_result=src)

    result = helper.submit(COMPILED_TEMPLATE)

    assert result == "OK"
    assert captured["loaded"] == '"myschema"."stg_orders"'
    assert captured["written_target"] == '"myschema"."py_model"'
    pd.testing.assert_frame_equal(captured["written_df"], src)


def test_submit_wraps_model_errors():
    captured = {}
    helper = _make_helper(captured)
    # a minimal compiled program whose model() raises
    program = (
        "def model(dbt, session):\n"
        "    raise ValueError('boom')\n"
        "class dbtObj:\n"
        "    def __init__(self, f):\n        pass\n"
        "dbt = dbtObj(load_df_function)\n"
        "df = model(dbt, session)\n"
        'teradata__write_df(df, "t")\n'
    )
    with pytest.raises(dbt_common.exceptions.DbtRuntimeError) as exc:
        helper.submit(program)
    assert "py_model" in str(exc.value)
    assert "boom" in str(exc.value)


# --------------------------------------------------------------------------- #
# _coerce_to_pandas
# --------------------------------------------------------------------------- #
def test_coerce_passes_pandas_through():
    helper = _make_raw_helper()
    df = pd.DataFrame({"a": [1]})
    assert helper._coerce_to_pandas(df) is df


def test_coerce_uses_to_pandas_when_available():
    helper = _make_raw_helper()
    inner = pd.DataFrame({"a": [1]})

    class FakeTdmlDF:
        def to_pandas(self):
            return inner

    assert helper._coerce_to_pandas(FakeTdmlDF()) is inner


def test_coerce_rejects_unsupported_type():
    helper = _make_raw_helper()
    with pytest.raises(dbt_common.exceptions.DbtRuntimeError):
        helper._coerce_to_pandas([1, 2, 3])


def test_coerce_rejects_to_pandas_returning_non_dataframe():
    helper = _make_raw_helper()

    class FakeTdmlDF:
        def to_pandas(self):
            return {"not": "a dataframe"}

    with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="to_pandas"):
        helper._coerce_to_pandas(FakeTdmlDF())


def test_coerce_wraps_to_pandas_errors():
    helper = _make_raw_helper()

    class FakeTdmlDF:
        def to_pandas(self):
            raise ValueError("boom")

    with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="to_pandas"):
        helper._coerce_to_pandas(FakeTdmlDF())


# --------------------------------------------------------------------------- #
# _write_dataframe SQL generation (connection mocked)
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, log):
        self._log = log

    def execute(self, sql):
        self._log.append(("execute", sql))

    def executemany(self, sql, rows):
        self._log.append(("executemany", sql, list(rows)))


class _FakeConn:
    def __init__(self, log):
        self._log = log

    def cursor(self):
        return _FakeCursor(self._log)

    def close(self):
        self._log.append(("close",))


def _install_fake_conn(helper, log):
    helper._connect = lambda: _FakeConn(log)
    # bypass the real drop (avoids importing teradatasql error class). Assign a
    # plain callable on the instance (not staticmethod, which only unwraps on a
    # class) so self._drop_if_exists(cursor, target) works on every Python.
    helper._drop_if_exists = lambda cursor, target: log.append(("drop", target))


def test_write_dataframe_generates_create_and_insert():
    log = []
    helper = _make_raw_helper()
    _install_fake_conn(helper, log)

    df = pd.DataFrame(
        {"id": pd.Series([1, 2], dtype="int64"), "name": ["a", "bb"]}
    )
    helper._write_dataframe(df, '"s"."t"')

    # Atomic build-then-swap: staging is dropped+created+loaded, then the
    # target is dropped and staging renamed into place.
    kinds = [row[0] for row in log]
    assert kinds == ["drop", "execute", "executemany", "drop", "execute", "close"]
    # staging is built first, target never touched until the swap
    assert log[0] == ("drop", '"s"."t__dbt_tmp"')
    create_sql = log[1][1]
    assert create_sql.startswith('CREATE MULTISET TABLE "s"."t__dbt_tmp"')
    assert '"id" BIGINT' in create_sql
    insert_sql = log[2][1]
    assert insert_sql.startswith(
        'INSERT INTO "s"."t__dbt_tmp" ("id", "name") VALUES (?, ?)'
    )
    assert log[2][2] == [[1, "a"], [2, "bb"]]
    # swap: drop the real target, then RENAME staging -> target
    assert log[3] == ("drop", '"s"."t"')
    assert log[4][1] == 'RENAME TABLE "s"."t__dbt_tmp" TO "s"."t"'


def test_write_dataframe_cleans_up_staging_on_failure():
    log = []
    helper = _make_raw_helper()

    class _BoomCursor(_FakeCursor):
        def executemany(self, sql, rows):
            self._log.append(("executemany", sql, list(rows)))
            raise RuntimeError("load blew up")

    class _BoomConn(_FakeConn):
        def cursor(self):
            return _BoomCursor(self._log)

    helper._connect = lambda: _BoomConn(log)
    drops = []
    helper._drop_if_exists = (
        lambda cursor, target: drops.append(target) or log.append(("drop", target))
    )

    df = pd.DataFrame({"id": pd.Series([1, 2], dtype="int64")})
    with pytest.raises(RuntimeError, match="load blew up"):
        helper._write_dataframe(df, '"s"."t"')

    # target was never dropped; staging was dropped for cleanup and connection closed
    assert '"s"."t"' not in drops
    assert drops.count('"s"."t__dbt_tmp"') == 2  # initial drop + cleanup drop
    assert log[-1] == ("close",)


def test_write_dataframe_clob_column_uses_no_primary_index():
    log = []
    helper = _make_raw_helper()
    _install_fake_conn(helper, log)
    df = pd.DataFrame({"big": ["x" * 40000]})   # -> CLOB
    helper._write_dataframe(df, '"s"."t"')
    create_sql = next(r[1] for r in log if r[0] == "execute")
    assert "CLOB" in create_sql
    assert "NO PRIMARY INDEX" in create_sql


def test_write_dataframe_no_primary_index_only_for_real_clob():
    # a non-CLOB column whose NAME contains "CLOB" must NOT trigger NO PRIMARY INDEX
    log = []
    helper = _make_raw_helper()
    _install_fake_conn(helper, log)
    df = pd.DataFrame({"MYCLOB": pd.Series([1, 2], dtype="int64")})
    helper._write_dataframe(df, '"s"."t"')
    create_sql = next(r[1] for r in log if r[0] == "execute")
    assert "NO PRIMARY INDEX" not in create_sql


def test_write_dataframe_rejects_duplicate_columns():
    helper = _make_raw_helper()
    df = pd.DataFrame([[1, 2], [3, 4]], columns=["a", "a"])
    with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="duplicate column"):
        helper._write_dataframe(df, '"s"."t"')


def test_write_dataframe_fastload_uses_single_executemany():
    # FastLoad must be one executemany into the empty staging table (no chunking),
    # otherwise the 2nd chunk fails with "must be empty for Fast Loading".
    log = []
    helper = _make_raw_helper()
    helper.use_fastload = True
    helper.batch_size = 2   # small, to prove fastload ignores chunking
    _install_fake_conn(helper, log)

    df = pd.DataFrame({"id": pd.Series([1, 2, 3, 4, 5], dtype="int64")})
    helper._write_dataframe(df, '"s"."t"')

    executemany_calls = [row for row in log if row[0] == "executemany"]
    assert len(executemany_calls) == 1                     # single call
    assert executemany_calls[0][1].startswith(
        '{fn teradata_try_fastload}INSERT INTO "s"."t__dbt_tmp"'
    )
    assert len(executemany_calls[0][2]) == 5               # all rows at once


def test_write_dataframe_batches_when_not_fastload():
    # the ordinary (non-fastload) path chunks at batch_size, no fastload prefix.
    log = []
    helper = _make_raw_helper()
    helper.use_fastload = False
    helper.batch_size = 2
    _install_fake_conn(helper, log)

    df = pd.DataFrame({"id": pd.Series([1, 2, 3, 4, 5], dtype="int64")})
    helper._write_dataframe(df, '"s"."t"')

    executemany_calls = [row for row in log if row[0] == "executemany"]
    assert len(executemany_calls) == 3                     # 5 rows / batch 2
    assert "teradata_try_fastload" not in executemany_calls[0][1]
    assert [len(c[2]) for c in executemany_calls] == [2, 2, 1]


def test_write_dataframe_empty_skips_insert():
    log = []
    helper = _make_raw_helper()
    _install_fake_conn(helper, log)

    df = pd.DataFrame({"id": pd.Series([], dtype="int64")})
    helper._write_dataframe(df, '"s"."t"')

    kinds = [row[0] for row in log]
    assert "executemany" not in kinds
    # staging drop+create, then swap (drop target + rename)
    assert kinds == ["drop", "execute", "drop", "execute", "close"]
    assert log[3][1] == 'RENAME TABLE "s"."t__dbt_tmp" TO "s"."t"'


# --------------------------------------------------------------------------- #
# session shim
# --------------------------------------------------------------------------- #
def test_session_table_and_call_delegate():
    seen = []
    session = _TeradataSession(lambda name: seen.append(name) or pd.DataFrame())
    session.table("rel_a")
    session("rel_b")
    assert seen == ["rel_a", "rel_b"]


# --------------------------------------------------------------------------- #
# _make_staging_relation (plan case D-01: __dbt_tmp naming for the swap)
# --------------------------------------------------------------------------- #
def test_make_staging_relation_schema_qualified():
    m = TeradataClientPythonJobHelper._make_staging_relation
    assert m('"myschema"."py_model"') == '"myschema"."py_model__dbt_tmp"'


def test_make_staging_relation_without_schema():
    m = TeradataClientPythonJobHelper._make_staging_relation
    assert m('"py_model"') == '"py_model__dbt_tmp"'


def test_make_staging_relation_custom_suffix():
    m = TeradataClientPythonJobHelper._make_staging_relation
    assert m('"s"."t"', suffix="__stage") == '"s"."t__stage"'


def test_make_staging_relation_preserves_embedded_quotes():
    # an identifier with an escaped embedded quote ("" in Teradata) must be kept
    # intact — the suffix goes before the closing quote, not via strip('"').
    m = TeradataClientPythonJobHelper._make_staging_relation
    assert m('"s"."a""b"') == '"s"."a""b__dbt_tmp"'


# --------------------------------------------------------------------------- #
# type-mapping edge cases (plan cases C-05 tz, C-08 >32000, C-10 mixed)
# --------------------------------------------------------------------------- #
def test_dtype_mapping_tz_aware_datetime_is_timestamp():
    s = pd.Series(pd.to_datetime(["2024-01-01", "2024-02-01"]).tz_localize("UTC"))
    assert pandas_dtype_to_teradata(s) == "TIMESTAMP"


def test_dtype_mapping_object_over_32000_is_clob():
    # a value longer than the 32000 VARCHAR limit -> CLOB (not a truncated VARCHAR)
    s = pd.Series(["a" * 40000], dtype="object")
    assert pandas_dtype_to_teradata(s) == "CLOB CHARACTER SET UNICODE"


def test_dtype_mapping_object_at_32000_is_varchar():
    s = pd.Series(["a" * 32000], dtype="object")
    assert pandas_dtype_to_teradata(s) == "VARCHAR(32000) CHARACTER SET UNICODE"


def test_dtype_mapping_mixed_object_uses_stringified_length():
    # object column mixing an int and a string -> sized to the longest str() form.
    s = pd.Series([1, "abc"], dtype="object")
    assert pandas_dtype_to_teradata(s) == "VARCHAR(3) CHARACTER SET UNICODE"


def test_dataframe_to_bindings_stringifies_mixed_object_column():
    # a mixed-type object column must bind every value as a string so the whole
    # column is one (VARCHAR) parameter type for executemany.
    df = pd.DataFrame({"mixed": pd.Series([1, "abc"], dtype="object")})
    bindings = dataframe_to_bindings(df)
    assert bindings == [["1"], ["abc"]]
    assert all(isinstance(b[0], str) for b in bindings)


# --------------------------------------------------------------------------- #
# return-type rejection breadth (plan case E-05)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "bad",
    [
        None,
        [1, 2, 3],
        {"a": 1},
        42,
        "a string",
        pd.Series([1, 2, 3]),
        np.array([1, 2, 3]),
    ],
)
def test_coerce_rejects_various_non_dataframes(bad):
    helper = _make_raw_helper()
    with pytest.raises(dbt_common.exceptions.DbtRuntimeError):
        helper._coerce_to_pandas(bad)


# --------------------------------------------------------------------------- #
# bindings for bool + datetime values (plan cases C-03, C-04)
# --------------------------------------------------------------------------- #
def test_dataframe_to_bindings_timedelta_becomes_pytimedelta():
    df = pd.DataFrame({"span": pd.to_timedelta(["1 days", "2 days 03:04:05"])})
    bindings = dataframe_to_bindings(df)
    v = bindings[0][0]
    assert isinstance(v, datetime.timedelta) and not isinstance(v, pd.Timedelta)
    assert v == datetime.timedelta(days=1)


def test_dataframe_to_bindings_tz_aware_datetime_becomes_naive():
    df = pd.DataFrame(
        {"ts": pd.to_datetime(["2024-01-01 10:00:00"]).tz_localize("UTC")}
    )
    v = dataframe_to_bindings(df)[0][0]
    assert isinstance(v, datetime.datetime) and not isinstance(v, pd.Timestamp)
    assert v.tzinfo is None
    assert v == datetime.datetime(2024, 1, 1, 10, 0, 0)


def test_dataframe_to_bindings_complex_column_is_stringified():
    # complex dtype maps to VARCHAR in the DDL, so its values must bind as
    # strings too (consistent), not as raw python complex objects.
    df = pd.DataFrame({"c": pd.Series([1 + 2j, 3 + 4j])})
    assert pandas_dtype_to_teradata(df["c"]).startswith("VARCHAR")
    bindings = dataframe_to_bindings(df)
    assert all(isinstance(b[0], str) for b in bindings)


def test_dataframe_to_bindings_bool_and_datetime():
    df = pd.DataFrame(
        {
            "b": pd.Series([True, False], dtype="bool"),
            "d": pd.to_datetime(["2024-01-01", "2024-02-01"]),
        }
    )
    bindings = dataframe_to_bindings(df)
    # numpy bool -> native python bool
    assert bindings[0][0] is True and isinstance(bindings[0][0], bool)
    assert bindings[1][0] is False
    # datetime value survives as a datetime-like (pd.Timestamp is a datetime)
    assert bindings[0][1] is not None
    assert isinstance(bindings[0][1], datetime.datetime)
