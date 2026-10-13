{% macro run_hooks(hooks, inside_transaction=True) %}
  {#-
    Override of dbt-core's global `run_hooks` macro (see
    dbt/include/global_project/macros/materializations/hooks.sql).

    The core implementation issues a literal `commit;` statement before
    running the first hook configured with `transaction: False`, so that
    "out of transaction" hooks (e.g. Postgres's `VACUUM`) can run outside
    the wrapping transaction. dbt-teradata's connection manager never
    opens a real dbt-managed transaction in the first place (begin()/
    commit()/add_begin_query()/add_commit_query() are all no-ops), so
    that commit is both unnecessary and, in TERA (BTET) transaction mode,
    invalid SQL (Error 3706: "COMMIT WORK not allowed for a DBC/SQL
    session"). Skip emitting it here.

    DRIFT WARNING: apart from that commit, this macro is a verbatim copy of
    dbt-core's, and `run_hooks` is *not* dispatched (there is no
    teradata__/default__ pair) -- dbt just resolves this package's macro
    ahead of the global project and silently shadows core's. So a change to
    core's implementation will not surface here as a conflict or a warning;
    this copy would simply keep running the stale body. Last synced against
    dbt-core 1.12.x. tests/unit/test_run_hooks_macro_drift.py re-derives
    "core minus the commit block" and fails if the two diverge; if it does
    fail, re-copy core's macro rather than patching this one in place.
  -#}
  {% for hook in hooks | selectattr('transaction', 'equalto', inside_transaction)  %}
    {% set rendered = render(hook.get('sql')) | trim %}
    {% if (rendered | length) > 0 %}
      {% call statement(auto_begin=inside_transaction) %}
        {{ rendered }}
      {% endcall %}
    {% endif %}
  {% endfor %}
{% endmacro %}
