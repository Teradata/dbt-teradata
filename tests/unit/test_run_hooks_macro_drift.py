"""Drift guard for dbt-teradata's forked `run_hooks` macro.

dbt/include/teradata/macros/materializations/hooks.sql redefines dbt-core's
global `run_hooks` macro. It is a verbatim copy of the core implementation with
exactly one change: the literal `commit;` statement that core emits before the
first `transaction: False` hook is not emitted.

That override is deliberate -- dbt-teradata's connection manager never opens a
dbt-managed transaction (begin/commit/add_begin_query/add_commit_query are all
no-ops), so the commit is unnecessary in ANSI mode and outright invalid in TERA
(BTET) mode, where it fails with "Error 3706: COMMIT WORK not allowed for a
DBC/SQL session".

The override is also *fragile*, which is why this test exists. `run_hooks` is a
plain global-project macro, not a dispatched one: there is no
`teradata__run_hooks`/`default__run_hooks` pair. dbt simply resolves the
adapter package's macro ahead of the global project's and silently shadows it.
So if dbt-core ever changes `run_hooks`, dbt-teradata keeps executing the old
body with no error, no warning, and no failing test -- hooks would quietly stop
behaving the way they do on every other adapter.

This test re-derives what the fork *should* be (core's current body, minus the
commit block) and asserts the fork still matches. When dbt-core changes the
macro, this fails with a diff, forcing a deliberate decision to re-fork rather
than letting the copy rot.

No database is required.
"""

import os
import re

import pytest

import dbt.include.global_project
import dbt.include.teradata


_HOOKS_RELPATH = os.path.join("macros", "materializations", "hooks.sql")

_CORE_HOOKS = os.path.join(dbt.include.global_project.PACKAGE_PATH, _HOOKS_RELPATH)
_TERADATA_HOOKS = os.path.join(dbt.include.teradata.PACKAGE_PATH, _HOOKS_RELPATH)

# The one and only block the fork is allowed to drop. Matched structurally
# rather than as a fixed string so that reindentation upstream doesn't produce
# a spurious failure.
_COMMIT_BLOCK = re.compile(
    r"\{%-?\s*if\s+not\s+inside_transaction\s+and\s+loop\.first\s*-?%\}"
    r".*?"
    r"\{%-?\s*endif\s*-?%\}",
    re.DOTALL,
)

_JINJA_COMMENT = re.compile(r"\{#.*?#\}", re.DOTALL)


def _read(path):
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def _extract_run_hooks(source):
    """Return just the `run_hooks` macro body from a hooks.sql file."""
    start = re.search(r"\{%-?\s*macro\s+run_hooks\s*\(", source)
    assert start is not None, "run_hooks macro not found"

    end = re.search(r"\{%-?\s*endmacro\s*-?%\}", source[start.start():])
    assert end is not None, "unterminated run_hooks macro"

    return source[start.start(): start.start() + end.end()]


def _normalize(macro_source):
    """Strip Jinja comments and make the comparison whitespace-insensitive."""
    without_comments = _JINJA_COMMENT.sub("", macro_source)
    lines = (line.strip() for line in without_comments.splitlines())
    return "\n".join(line for line in lines if line)


@pytest.fixture(scope="module")
def core_macro():
    return _extract_run_hooks(_read(_CORE_HOOKS))


@pytest.fixture(scope="module")
def teradata_macro():
    return _extract_run_hooks(_read(_TERADATA_HOOKS))


def test_core_still_emits_the_commit_we_strip(core_macro):
    """The premise of the fork: core emits a bare `commit;` we must not run.

    If this ever fails, dbt-core has stopped emitting the commit and the
    override may no longer be needed at all.
    """
    assert _COMMIT_BLOCK.search(core_macro) is not None, (
        "dbt-core's run_hooks no longer contains the out-of-transaction commit "
        "block. The dbt-teradata override may now be unnecessary -- re-check "
        "whether hooks.sql can simply be deleted."
    )
    assert "commit;" in _COMMIT_BLOCK.search(core_macro).group(0)


def test_teradata_override_does_not_emit_commit(teradata_macro):
    """The actual bug being prevented: `commit;` is invalid in TERA mode.

    Comments are stripped first -- the override's own docstring necessarily
    mentions the statement it exists to suppress.
    """
    executable = _JINJA_COMMENT.sub("", teradata_macro)
    assert "commit;" not in executable.lower(), (
        "dbt-teradata's run_hooks must not emit a literal `commit;` -- it "
        "fails in TERA (BTET) mode with Error 3706."
    )


def test_override_matches_core_apart_from_the_commit(core_macro, teradata_macro):
    """The fork must stay byte-faithful to core except for the commit block.

    Any other difference means either dbt-core changed `run_hooks` (and the
    fork needs to be refreshed) or someone edited the fork directly (and that
    edit belongs upstream, or needs to be justified here).
    """
    expected = _normalize(_COMMIT_BLOCK.sub("", core_macro))
    actual = _normalize(teradata_macro)

    assert actual == expected, (
        "dbt-teradata's forked run_hooks has drifted from dbt-core's.\n\n"
        "Re-copy dbt-core's macro from:\n"
        f"  {_CORE_HOOKS}\n"
        "into:\n"
        f"  {_TERADATA_HOOKS}\n"
        "removing only the `commit;` block, then re-run this test.\n\n"
        f"--- expected (core minus commit block) ---\n{expected}\n\n"
        f"--- actual (dbt-teradata) ---\n{actual}"
    )
