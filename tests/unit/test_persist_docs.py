"""Unit tests for persist_docs macros (IDE-26225).

Pure unit tests (no database required). They render the Jinja macros in
dbt/include/teradata/macros/persist_docs.sql with lightweight stubs for the
dbt-provided globals (`exceptions`, `var`, `config`) and assert behavior:

  1. teradata_escape_comment: single-quote escaping, 255-char truncation,
     configurable limit via var, and non-string rejection.
  2. teradata__alter_relation_comment: emits COMMENT ON TABLE vs VIEW vs FUNCTION.
  3. teradata__validate_doc_columns: quote-aware, case-sensitive filtering
     plus the "columns not present" warning.
  4. teradata__persist_docs: column-level persist_docs is skipped with a
     warning for function (UDF) relations, since Teradata has no per-argument
     comment DDL.
"""

import os

import pytest
from jinja2 import Environment


_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_PERSIST_DOCS = os.path.join(
    _REPO_ROOT, "dbt", "include", "teradata", "macros", "persist_docs.sql"
)


class _MacroReturn(Exception):
    """Emulates dbt's `return()` which raises to hand a value back from a macro."""

    def __init__(self, value):
        self.value = value


def _call_returning(macro, *args):
    """Invoke a macro that ends in `{{ return(x) }}` and recover x."""
    try:
        macro(*args)
    except _MacroReturn as r:
        return r.value
    raise AssertionError("macro did not call return()")


class _Exceptions:
    """Stub for dbt's `exceptions` global. Records warnings and raises on error."""

    def __init__(self):
        self.warnings = []

    def warn(self, msg):
        self.warnings.append(str(msg))
        return ""

    def raise_compiler_error(self, msg):
        raise ValueError(str(msg))


class _Relation:
    """Minimal stand-in for a dbt relation: renders to a quoted name and has a type."""

    def __init__(self, name='"db"."obj"', rtype="table"):
        self._name = name
        self.type = rtype

    def render(self):
        return self._name

    def __str__(self):
        return self._name


class _Config:
    """Stub for dbt's per-model `config` global used by teradata__persist_docs."""

    def __init__(self, catalog_name=None, persist_relation=False, persist_columns=False):
        self._catalog_name = catalog_name
        self._persist_relation = persist_relation
        self._persist_columns = persist_columns

    def get(self, key, default=None):
        if key == "catalog_name":
            return self._catalog_name
        return default

    def persist_relation_docs(self):
        return self._persist_relation

    def persist_column_docs(self):
        return self._persist_columns


class _Model:
    """Minimal stand-in for the `model` global: description + columns dict."""

    def __init__(self, description="", columns=None):
        self.description = description
        self.columns = columns or {}


def _load_macros(max_comment_length=255, config=None):
    """Load persist_docs.sql and return its rendered Jinja module plus the exceptions stub."""
    exc = _Exceptions()
    env = Environment(extensions=["jinja2.ext.do"])
    env.globals["exceptions"] = exc
    env.globals["var"] = lambda key, default=None: (
        max_comment_length if key == "teradata_max_comment_length" else default
    )
    env.globals["config"] = config if config is not None else _Config()

    def _return(value):
        raise _MacroReturn(value)

    env.globals["return"] = _return
    with open(_PERSIST_DOCS, encoding="utf-8") as fh:
        module = env.from_string(fh.read()).module
    return module, exc


# ---------------------------------------------------------------------------
# teradata_escape_comment
# ---------------------------------------------------------------------------
class TestEscapeComment:
    def test_wraps_in_single_quotes(self):
        mod, _ = _load_macros()
        assert str(mod.teradata_escape_comment("hello")) == "'hello'"

    def test_doubles_single_quotes(self):
        mod, _ = _load_macros()
        # o'brien -> 'o''brien'
        assert str(mod.teradata_escape_comment("o'brien")) == "'o''brien'"

    def test_triple_quoted_token_roundtrips(self):
        mod, _ = _load_macros()
        text = "'''abc'''"
        assert str(mod.teradata_escape_comment(text)) == "'" + text.replace("'", "''") + "'"

    def test_preserves_newlines_and_sql_comment_tokens(self):
        mod, _ = _load_macros()
        text = "line1\n-- dashcomment\n/* block */"
        out = str(mod.teradata_escape_comment(text))
        assert out == "'" + text + "'"
        assert "\n" in out and "--" in out and "/* block */" in out

    def test_truncates_at_255_and_warns(self):
        mod, exc = _load_macros()
        out = str(mod.teradata_escape_comment("x" * 400))
        # 255 x's wrapped in quotes
        assert out == "'" + "x" * 255 + "'"
        assert len(out) == 257
        assert any("truncating" in w for w in exc.warnings)

    def test_no_truncation_at_exactly_255(self):
        mod, exc = _load_macros()
        out = str(mod.teradata_escape_comment("y" * 255))
        assert out == "'" + "y" * 255 + "'"
        assert exc.warnings == []

    def test_respects_configurable_limit(self):
        mod, exc = _load_macros(max_comment_length=10)
        out = str(mod.teradata_escape_comment("z" * 50))
        assert out == "'" + "z" * 10 + "'"
        assert any("10 characters" in w for w in exc.warnings)

    def test_non_string_raises(self):
        mod, _ = _load_macros()
        with pytest.raises(ValueError):
            mod.teradata_escape_comment(123)

    def test_null_limit_falls_back_to_255(self):
        # var set to null/None must not coerce to 0 (which would empty the comment)
        mod, exc = _load_macros(max_comment_length=None)
        out = str(mod.teradata_escape_comment("q" * 300))
        assert out == "'" + "q" * 255 + "'"

    def test_non_positive_limit_falls_back_to_255(self):
        mod, exc = _load_macros(max_comment_length=0)
        out = str(mod.teradata_escape_comment("q" * 300))
        assert out == "'" + "q" * 255 + "'"

    def test_non_numeric_limit_falls_back_to_255(self):
        mod, exc = _load_macros(max_comment_length="not-a-number")
        out = str(mod.teradata_escape_comment("q" * 300))
        assert out == "'" + "q" * 255 + "'"


# ---------------------------------------------------------------------------
# teradata__alter_relation_comment
# ---------------------------------------------------------------------------
class TestAlterRelationComment:
    def test_table_emits_comment_on_table(self):
        mod, _ = _load_macros()
        sql = str(mod.teradata__alter_relation_comment(_Relation(rtype="table"), "desc")).strip()
        assert sql.lower().startswith("comment on table")
        assert sql.endswith("as 'desc'")

    def test_view_emits_comment_on_view(self):
        mod, _ = _load_macros()
        sql = str(mod.teradata__alter_relation_comment(_Relation(rtype="view"), "desc")).strip()
        assert sql.lower().startswith("comment on view")

    def test_snapshot_treated_as_table(self):
        # snapshot is a real dbt relation type; it maps to COMMENT ON TABLE.
        mod, _ = _load_macros()
        sql = str(mod.teradata__alter_relation_comment(_Relation(rtype="snapshot"), "d")).strip()
        assert sql.lower().startswith("comment on table")

    def test_function_emits_comment_on_function(self):
        mod, _ = _load_macros()
        sql = str(
            mod.teradata__alter_relation_comment(_Relation(rtype="function"), "desc")
        ).strip()
        assert sql.lower().startswith("comment on function")
        assert sql.endswith("as 'desc'")

    def test_escapes_embedded_quote(self):
        mod, _ = _load_macros()
        sql = str(mod.teradata__alter_relation_comment(_Relation(), "a'b")).strip()
        assert sql.endswith("as 'a''b'")


# ---------------------------------------------------------------------------
# teradata__validate_doc_columns
# ---------------------------------------------------------------------------
def _col(quote=False):
    return {"quote": quote, "description": "d"}


class TestValidateDocColumns:
    def test_unquoted_matches_case_insensitively(self):
        mod, exc = _load_macros()
        filtered = _call_returning(
            mod.teradata__validate_doc_columns, _Relation(), {"ID": _col()}, ["id"]
        )
        assert list(filtered.keys()) == ["ID"]
        assert exc.warnings == []

    def test_quoted_matches_case_sensitively(self):
        mod, exc = _load_macros()
        # quoted "MyCol" is not present as physical "mycol" -> filtered out + warning
        filtered = _call_returning(
            mod.teradata__validate_doc_columns,
            _Relation(),
            {"MyCol": _col(quote=True)},
            ["mycol"],
        )
        assert list(filtered.keys()) == []
        assert any("MyCol" in w for w in exc.warnings)

    def test_missing_column_warns_with_expected_message(self):
        mod, exc = _load_macros()
        _call_returning(
            mod.teradata__validate_doc_columns, _Relation(), {"ghost": _col()}, ["id", "nm"]
        )
        assert any(
            "The following columns are specified in the schema but are not present "
            "in the database: ghost" in w
            for w in exc.warnings
        )

    def test_mixed_present_and_missing(self):
        mod, exc = _load_macros()
        filtered = _call_returning(
            mod.teradata__validate_doc_columns,
            _Relation(),
            {"id": _col(), "ghost": _col()},
            ["id"],
        )
        assert list(filtered.keys()) == ["id"]
        assert any("ghost" in w for w in exc.warnings)


# ---------------------------------------------------------------------------
# teradata__persist_docs: column docs are skipped (with a warning) for
# function (UDF) relations, since Teradata has no per-argument comment DDL.
# ---------------------------------------------------------------------------
class TestPersistDocsFunctionColumnsSkipped:
    def test_function_with_columns_warns_and_skips(self):
        # for_relation=False isolates the column-docs branch: no run_query/statement
        # stubs are needed since that code path is never reached for a function.
        config = _Config(persist_columns=True)
        mod, exc = _load_macros(config=config)
        model = _Model(columns={"x": _col()})
        mod.teradata__persist_docs(_Relation(rtype="function"), model, False, True)
        assert any(
            "persist_docs 'columns' config is not supported for Teradata functions" in w
            for w in exc.warnings
        )

    def test_function_without_columns_is_silent(self):
        # model.columns is empty (the common case: functions.yml has no `columns:`
        # block) -> the condition is falsy and no warning is emitted at all.
        config = _Config(persist_columns=True)
        mod, exc = _load_macros(config=config)
        model = _Model(columns={})
        mod.teradata__persist_docs(_Relation(rtype="function"), model, False, True)
        assert exc.warnings == []

    def test_columns_disabled_is_silent_even_with_columns(self):
        config = _Config(persist_columns=False)
        mod, exc = _load_macros(config=config)
        model = _Model(columns={"x": _col()})
        mod.teradata__persist_docs(_Relation(rtype="function"), model, False, True)
        assert exc.warnings == []
