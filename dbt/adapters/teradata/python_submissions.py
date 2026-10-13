"""Client-side Python model support for dbt-teradata (IDE-24196).

Teradata (non-Lake) has no in-database Python runtime that dbt can submit a
dataframe-producing script to (unlike Snowflake/Snowpark or Oracle/OML4Py).
The client-side strategy therefore runs the model's Python **in the dbt runner
process**: upstream ``ref()``/``source()`` relations are read back over
``teradatasql`` into pandas DataFrames, the user's ``model()`` transforms them
with pandas, and the resulting DataFrame is written back to the target table
over ``teradatasql``.

This module intentionally keeps all database interaction behind small, mockable
methods (``_load_relation`` / ``_write_dataframe`` / ``_connect``) so the
code-assembly and type-mapping logic can be unit-tested without a live Teradata
instance.
"""

from typing import Any, Dict, List, Optional

import dbt_common.exceptions
from dbt.adapters.base import PythonJobHelper
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.teradata.connections import TeradataConnectionManager

logger = AdapterLogger("teradata")

# teradatasql executemany batches; keep in sync with the seed materialization's
# teradata__get_batch_size() default so behaviour is consistent across features.
DEFAULT_BATCH_SIZE = 2536

# Fallback VARCHAR length for all-null / unknown object columns.
DEFAULT_VARCHAR_LENGTH = 255


def pandas_dtype_to_teradata(series) -> str:
    """Map a pandas Series to a Teradata column type string.

    Kept dependency-light: ``pandas`` is imported lazily by the caller and the
    dtype is inspected via its ``kind`` so this works for numpy-backed dtypes.
    """
    import pandas as pd  # local import: pandas is only needed for python models

    dtype = series.dtype
    kind = getattr(dtype, "kind", None)

    if kind == "b":
        return "BYTEINT"
    if kind in ("i", "u"):
        return "BIGINT"
    if kind == "f":
        return "FLOAT"
    if kind == "M":
        return "TIMESTAMP"
    if kind == "m":
        return "INTERVAL DAY(4) TO SECOND"
    # object / string / everything else -> VARCHAR sized to the longest value,
    # or CLOB when a value exceeds the 32000-char VARCHAR limit.
    non_null = series.dropna()
    if len(non_null) == 0:
        return f"VARCHAR({DEFAULT_VARCHAR_LENGTH}) CHARACTER SET UNICODE"
    try:
        length = int(non_null.astype(str).map(len).max())
    except (TypeError, ValueError):
        return f"VARCHAR({DEFAULT_VARCHAR_LENGTH}) CHARACTER SET UNICODE"
    if length > 32000:
        return "CLOB CHARACTER SET UNICODE"
    return f"VARCHAR({max(1, length)}) CHARACTER SET UNICODE"


def _quote_ident(name) -> str:
    """Quote a Teradata identifier, escaping any embedded double quotes."""
    return '"' + str(name).replace('"', '""') + '"'


def infer_column_types(df) -> "dict":
    """Map each DataFrame column to its inferred Teradata type (computed once)."""
    return {col: pandas_dtype_to_teradata(df[col]) for col in df.columns}


def ddl_from_column_types(col_types) -> str:
    """Build the parenthesised CREATE TABLE column list from a {col: type} map."""
    return ",\n    ".join(
        f"{_quote_ident(col)} {td_type}" for col, td_type in col_types.items()
    )


def build_column_ddl(df) -> str:
    """Return the parenthesised column list for a CREATE TABLE from a DataFrame."""
    return ddl_from_column_types(infer_column_types(df))


def dataframe_to_bindings(df) -> List[list]:
    """Convert a pandas DataFrame to a list of row binding lists.

    numpy scalar types are coerced to native Python types and NaN/NaT to None so
    the values are accepted by the teradatasql driver. Values in object-dtype
    columns (which map to VARCHAR/CLOB) are coerced to ``str`` so every value in
    a column binds as the same type — otherwise a column that mixes python types
    (e.g. an int and a str) makes ``executemany`` reject the batch.
    """
    import pandas as pd

    # column positions bound as strings: object columns and complex ("c"), both
    # of which map to VARCHAR in pandas_dtype_to_teradata — keep the bound value
    # type consistent with the declared column type.
    str_cols = {
        i for i, dt in enumerate(df.dtypes) if getattr(dt, "kind", None) in ("O", "c")
    }
    records = []
    for row in df.itertuples(index=False, name=None):
        binding = []
        for i, value in enumerate(row):
            if value is None or (pd.isna(value) if _is_scalar_na(value) else False):
                binding.append(None)
            elif i in str_cols:  # object column -> VARCHAR/CLOB: bind as string
                binding.append(str(value))
            elif isinstance(value, pd.Timestamp):
                # -> TIMESTAMP: bind a plain datetime; drop tz (documented) so a
                # tz-aware value doesn't depend on driver-specific tz handling.
                ts = value.tz_localize(None) if value.tzinfo is not None else value
                binding.append(ts.to_pydatetime())
            elif isinstance(value, pd.Timedelta):
                binding.append(value.to_pytimedelta())  # -> INTERVAL
            elif hasattr(value, "item"):  # numpy scalar -> python scalar
                binding.append(value.item())
            else:
                binding.append(value)
        records.append(binding)
    return records


def _is_scalar_na(value) -> bool:
    """pd.isna raises/returns arrays for non-scalars; guard for scalars only."""
    return not hasattr(value, "__len__") or isinstance(value, (str, bytes))


def _as_bool(value) -> bool:
    """Coerce a config value (bool, or common string forms) to a bool.

    ``bool("false")`` is truthy, so a YAML config like ``use_fastload: "false"``
    would otherwise enable the flag. Normalize common string spellings.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return bool(value)


class TeradataPythonJobHelper(PythonJobHelper):
    """Base helper: holds parsed model + credentials, defines the submit contract."""

    def __init__(self, parsed_model: Dict, credentials) -> None:
        self.parsed_model = parsed_model
        self.credentials = credentials
        self.identifier = parsed_model["alias"]
        config = parsed_model.get("config", {})
        self.batch_size = self._parse_batch_size(config.get("batch_size", DEFAULT_BATCH_SIZE))
        self.use_fastload = _as_bool(config.get("use_fastload", False))

    def _parse_batch_size(self, value) -> int:
        """batch_size is used as a range() step; reject non-int/<=0 with a clear error."""
        try:
            batch_size = int(value)
        except (TypeError, ValueError):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Python model {self.identifier}: batch_size must be a positive "
                f"integer; got {value!r}."
            )
        if batch_size <= 0:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Python model {self.identifier}: batch_size must be a positive "
                f"integer; got {batch_size!r}."
            )
        return batch_size

    def submit(self, compiled_code: str) -> Any:
        raise NotImplementedError("Subclasses must implement submit()")


class TeradataClientPythonJobHelper(TeradataPythonJobHelper):
    """Executes the compiled Python model in the dbt runner process (client-side)."""

    def submit(self, compiled_code: str) -> Any:
        exec_globals: Dict[str, Any] = {
            "__name__": "__dbt_teradata_python_model__",
            "load_df_function": self._load_relation,
            "session": _TeradataSession(self._load_relation),
            "teradata__write_df": self._write_dataframe,
        }
        try:
            exec(compiled_code, exec_globals)  # noqa: S102 - trusted dbt-compiled code
        except dbt_common.exceptions.DbtRuntimeError:
            raise
        except Exception as exc:  # surface any user model error as a dbt runtime error
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Error running Python model {self.identifier}: {exc}"
            ) from exc
        return "OK"

    # -- database interaction (mocked in unit tests) -------------------------

    def _connect(self):
        import teradatasql

        kwargs = TeradataConnectionManager.get_connection_kwargs(self.credentials)
        return teradatasql.connect(**kwargs)

    def _load_relation(self, relation_name: str):
        """Read a relation into a pandas DataFrame via a SELECT."""
        import pandas as pd

        sql = f"SELECT * FROM {relation_name}"
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [d[0] for d in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return pd.DataFrame.from_records(rows, columns=columns)
        finally:
            conn.close()

    def _write_dataframe(self, df, target_relation: str) -> None:
        """Build the target table atomically from the DataFrame.

        The result is first built under a ``__dbt_tmp`` staging name and fully
        loaded; only once the load succeeds is the existing target dropped and
        the staging table renamed into place. This mirrors the build-then-swap
        pattern of the SQL table materialization (see
        ``teradata__rename_relation``) so a failed or cancelled load never
        leaves the target dropped or half-populated. On any failure the staging
        table is cleaned up.
        """
        df = self._coerce_to_pandas(df)

        if df.columns.duplicated().any():
            dupes = sorted(set(df.columns[df.columns.duplicated()].tolist()))
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Python model {self.identifier} returned duplicate column "
                f"name(s): {dupes}. Column names must be unique."
            )

        staging_relation = self._make_staging_relation(target_relation)

        col_types = infer_column_types(df)  # inferred once, reused for DDL + CLOB check
        column_ddl = ddl_from_column_types(col_types)
        create_sql = f"CREATE MULTISET TABLE {staging_relation} (\n    {column_ddl}\n)"
        # A LOB column can't be a primary index; Teradata defaults the first
        # column as the PI, so tables containing a CLOB must be NO PRIMARY INDEX.
        # Decide from the inferred column *types*, not the rendered DDL string
        # (a column *name* could contain the text "CLOB" and false-positive).
        if any(t.startswith("CLOB") for t in col_types.values()):
            create_sql += "\nNO PRIMARY INDEX"
        insert_cols = ", ".join(_quote_ident(c) for c in df.columns)
        placeholders = ", ".join("?" for _ in df.columns)
        prefix = "{fn teradata_try_fastload}" if self.use_fastload else ""
        insert_sql = (
            f"{prefix}INSERT INTO {staging_relation} ({insert_cols}) "
            f"VALUES ({placeholders})"
        )
        bindings = dataframe_to_bindings(df)

        conn = self._connect()
        try:
            cursor = conn.cursor()
            try:
                # Build + load the new data under the staging name; the target
                # is untouched until this fully succeeds.
                self._drop_if_exists(cursor, staging_relation)
                cursor.execute(create_sql)
                if bindings:
                    if self.use_fastload:
                        # FastLoad requires a SINGLE executemany into an empty
                        # table. Chunking would fast-load+commit the first chunk,
                        # then fail on the next ("[Error 2636] ... must be empty
                        # for Fast Loading"), so load all rows in one call.
                        cursor.executemany(insert_sql, bindings)
                    else:
                        for start in range(0, len(bindings), self.batch_size):
                            chunk = bindings[start : start + self.batch_size]
                            cursor.executemany(insert_sql, chunk)
                # Swap: drop the old target and promote staging into its place.
                self._drop_if_exists(cursor, target_relation)
                cursor.execute(
                    f"RENAME TABLE {staging_relation} TO {target_relation}"
                )
            except BaseException:
                # Best-effort cleanup so a failed/cancelled run does not leave a
                # stray staging table behind. BaseException so KeyboardInterrupt
                # (dbt cancel) is also cleaned up before re-raising. The cleanup
                # must never mask the original error, so its own failures are
                # swallowed.
                try:
                    self._drop_if_exists(cursor, staging_relation)
                except Exception:  # noqa: BLE001 - cleanup is best-effort
                    pass
                raise
        finally:
            conn.close()

    @staticmethod
    def _make_staging_relation(target_relation: str, suffix: str = "__dbt_tmp") -> str:
        """Return the staging relation name for a quoted target relation.

        Appends ``suffix`` to the (quoted) identifier part, preserving the
        schema qualifier: ``"schema"."tbl"`` -> ``"schema"."tbl__dbt_tmp"``.
        """
        parts = target_relation.rsplit(".", 1)
        last = parts[-1].strip()
        if len(last) >= 2 and last.startswith('"') and last.endswith('"'):
            # already quoted: insert the suffix before the closing quote so the
            # original token (including any escaped "" inside it) is preserved.
            parts[-1] = last[:-1] + suffix + '"'
        else:
            parts[-1] = f'"{last}{suffix}"'
        return ".".join(parts)

    @staticmethod
    def _drop_if_exists(cursor, target_relation: str) -> None:
        """Teradata has no DROP TABLE IF EXISTS; ignore 'object does not exist'."""
        import teradatasql

        try:
            cursor.execute(f"DROP TABLE {target_relation}")
        except teradatasql.Error as exc:  # [Error 3807] = object does not exist
            if "[Error 3807]" not in str(exc):
                raise

    def _coerce_to_pandas(self, df):
        """Accept a pandas DataFrame, or best-effort convert a teradataml one."""
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            return df
        # teradataml DataFrame (Track B / advanced users) exposes to_pandas().
        to_pandas = getattr(df, "to_pandas", None)
        if callable(to_pandas):
            try:
                converted = to_pandas()
            except Exception as exc:
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"Python model {self.identifier}: calling to_pandas() on the "
                    f"returned {type(df)!r} failed: {exc}"
                ) from exc
            if not isinstance(converted, pd.DataFrame):
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"Python model {self.identifier}: to_pandas() must return a "
                    f"pandas DataFrame; got {type(converted)!r}."
                )
            return converted
        raise dbt_common.exceptions.DbtRuntimeError(
            f"Python model {self.identifier} must return a pandas DataFrame; "
            f"got {type(df)!r}."
        )


class _TeradataSession:
    """Minimal ``session`` object passed to ``model(dbt, session)``.

    Not a Spark/Snowpark session. It exposes ``.table(name)`` and is callable so
    advanced users can materialize an upstream relation to pandas directly.
    """

    def __init__(self, load_df_function) -> None:
        self._load_df_function = load_df_function

    def table(self, relation_name: str):
        return self._load_df_function(relation_name)

    def __call__(self, relation_name: str):
        return self._load_df_function(relation_name)
