import re

import pytest

from dbt.tests.adapter.incremental import fixtures as inc_fixtures
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeSetup,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
    models__delete_insert_incremental_predicates_sql,
)
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
    models__duplicated_unary_unique_key_list_sql,
    models__empty_str_unique_key_sql,
    models__empty_unique_key_list_sql,
    models__no_unique_key_sql,
    models__nontyped_trinary_unique_key_list_sql,
    models__not_found_unique_key_list_sql,
    models__not_found_unique_key_sql,
    models__str_unique_key_sql,
    models__trinary_unique_key_list_sql,
    models__unary_unique_key_list_sql,
    models__expected__one_str__overwrite_sql,
    models__expected__unique_key_list__inplace_overwrite_sql,
)
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import BaseMergeExcludeColumns


# Teradata Error 3888: UNION/INTERSECT/MINUS requires each branch to reference a
# table. Append a harmless `from (select 1 as n) as t` to any UNION/UNION ALL
# branch that is a bare literal SELECT with no FROM clause of its own. Also cast a
# trailing bare 'YYYY-MM-DD' literal to DATE, and a bare `null` literal to
# VARCHAR(20): Teradata (Error 3654, "Corresponding select-list expressions are
# incompatible") requires every UNION branch's column to share the same type, and
# these fixtures mix an explicitly-typed first branch (DATE) with later bare
# VARCHAR/NULL literals in the same column position.
def _teradata_safe_bare_union(sql: str) -> str:
    parts = re.split(r"(\bunion\s+all\b|\bunion\b)", sql, flags=re.IGNORECASE)
    fixed = []
    for i, part in enumerate(parts):
        if i % 2 == 0 and re.search(r"\bselect\b", part, re.IGNORECASE) and not re.search(
            r"\bfrom\b", part, re.IGNORECASE
        ):
            part = re.sub(
                r",'(\d{4}-\d{2}-\d{2})'\s*$",
                r",cast('\1' as date)",
                part.rstrip("\n \t"),
            )
            part = re.sub(
                r",\s*null\s*,",
                ", cast(null as varchar(20)),",
                part,
                flags=re.IGNORECASE,
            )
            part = part + " from (select 1 as n) as t\n"
        fixed.append(part)
    return "".join(fixed)


# inc_fixtures._MODELS__A uses bare literal `union all select ...` branches; wrap each
# in `from (select 1) as t` so Teradata accepts the UNION. Also cast field3 to
# integer explicitly: Teradata infers a literal column's type from the first UNION
# branch (111 fits in BYTEINT), so later, larger literals (222..666) overflow that
# narrow inferred type (Error 2616) once the branches are combined.
_MODELS__A_TD = inc_fixtures._MODELS__A.replace(
    "\n    select 1 as id, 'aaa' as field1, 'bbb' as field2, 111 as field3, 'TTT' as field4"
    "\n    union all select 2 as id, 'ccc' as field1, 'ddd' as field2, 222 as field3, 'UUU' as field4"
    "\n    union all select 3 as id, 'eee' as field1, 'fff' as field2, 333 as field3, 'VVV' as field4"
    "\n    union all select 4 as id, 'ggg' as field1, 'hhh' as field2, 444 as field3, 'WWW' as field4"
    "\n    union all select 5 as id, 'iii' as field1, 'jjj' as field2, 555 as field3, 'XXX' as field4"
    "\n    union all select 6 as id, 'kkk' as field1, 'lll' as field2, 666 as field3, 'YYY' as field4\n",
    "\n    select 1 as id, 'aaa' as field1, 'bbb' as field2, cast(111 as integer) as field3, 'TTT' as field4 from (select 1 as n) as t"
    "\n    union all select 2 as id, 'ccc' as field1, 'ddd' as field2, cast(222 as integer) as field3, 'UUU' as field4 from (select 1 as n) as t"
    "\n    union all select 3 as id, 'eee' as field1, 'fff' as field2, cast(333 as integer) as field3, 'VVV' as field4 from (select 1 as n) as t"
    "\n    union all select 4 as id, 'ggg' as field1, 'hhh' as field2, cast(444 as integer) as field3, 'WWW' as field4 from (select 1 as n) as t"
    "\n    union all select 5 as id, 'iii' as field1, 'jjj' as field2, cast(555 as integer) as field3, 'XXX' as field4 from (select 1 as n) as t"
    "\n    union all select 6 as id, 'kkk' as field1, 'lll' as field2, cast(666 as integer) as field3, 'YYY' as field4 from (select 1 as n) as t\n",
)


# _MODELS__INCREMENTAL_IGNORE uses "LIMIT 3" in the first-run branch.
# Teradata has no LIMIT. Use "TOP 3" for a deterministic 3-row cap: Teradata's
# SAMPLE n (an integer literal) does return exactly n rows rather than a
# percentage, but which n rows it returns is randomized per execution, which
# would make this fixture's row selection non-deterministic across runs.
_INCREMENTAL_IGNORE_TD = inc_fixtures._MODELS__INCREMENTAL_IGNORE.replace(
    "SELECT id, field1, field2 FROM source_data LIMIT 3",
    "SELECT TOP 3 id, field1, field2 FROM source_data",
)

# _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET ends with a bare `order by id`.
# Teradata's table materialization wraps this in `CREATE TABLE AS (SELECT ...)`,
# and Teradata (Error 3706) disallows ORDER BY inside such subqueries. Drop the
# clause: it has no effect on the row-equality assertions this fixture is used for.
_INCREMENTAL_SYNC_ALL_COLUMNS_TARGET_TD = inc_fixtures._MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET.replace(
    "\nfrom source_data\norder by id\n", "\nfrom source_data\n"
)

# Same `order by id` issue as _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET above.
_INCREMENTAL_SYNC_REMOVE_ONLY_TARGET_TD = inc_fixtures._MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET.replace(
    "\nfrom source_data\norder by id\n", "\nfrom source_data\n"
)

_SCHEMA_CHANGE_MODELS = {
    "incremental_sync_remove_only.sql": inc_fixtures._MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
    "incremental_ignore.sql": _INCREMENTAL_IGNORE_TD,
    "incremental_sync_remove_only_target.sql": _INCREMENTAL_SYNC_REMOVE_ONLY_TARGET_TD,
    "incremental_ignore_target.sql": inc_fixtures._MODELS__INCREMENTAL_IGNORE_TARGET,
    "incremental_fail.sql": inc_fixtures._MODELS__INCREMENTAL_FAIL,
    "incremental_sync_all_columns.sql": inc_fixtures._MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
    "incremental_sync_all_columns_target.sql": _INCREMENTAL_SYNC_ALL_COLUMNS_TARGET_TD,
    "incremental_append_new_columns_remove_one.sql": inc_fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
    "model_a.sql": _MODELS__A_TD,
    "incremental_append_new_columns_target.sql": inc_fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
    "incremental_append_new_columns.sql": inc_fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
    "incremental_append_new_columns_remove_one_target.sql": inc_fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
}


class TestIncrementalOnSchemaChangeTeradata(BaseIncrementalOnSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return _SCHEMA_CHANGE_MODELS


class TestIncrementalOnSchemaChangeSetupTeradata(BaseIncrementalOnSchemaChangeSetup):
    @pytest.fixture(scope="class")
    def models(self):
        return _SCHEMA_CHANGE_MODELS


# Teradata Error 3888: UNION/INTERSECT/MINUS requires each branch to reference a table.
# Replace bare SELECT literals with SELECT ... FROM (SELECT 1) AS t. Also cast msg/color
# to a fixed-width VARCHAR(20): Teradata infers a UNION'd literal column's width from the
# first branch alone (e.g. 'hey' -> VARCHAR(3), 'blue' -> VARCHAR(4)), so later, wider
# literals in the same UNION ('anyway', 'purple') get silently truncated instead of
# raising an error.
_PREDICATES_MODEL_TD = models__delete_insert_incremental_predicates_sql.replace(
    "select 1 as id, 'hello' as msg, 'blue' as color\nunion all\nselect 2 as id, 'goodbye' as msg, 'red' as color",
    "select 1 as id, cast('hello' as varchar(20)) as msg, cast('blue' as varchar(20)) as color from (select 1 as n) as t"
    "\nunion all\nselect 2 as id, cast('goodbye' as varchar(20)) as msg, cast('red' as varchar(20)) as color from (select 1 as n) as t",
).replace(
    "select 1 as id, 'hey' as msg, 'blue' as color\nunion all\nselect 2 as id, 'yo' as msg, 'green' as color\nunion all\nselect 3 as id, 'anyway' as msg, 'purple' as color",
    "select 1 as id, cast('hey' as varchar(20)) as msg, cast('blue' as varchar(20)) as color from (select 1 as n) as t"
    "\nunion all\nselect 2 as id, cast('yo' as varchar(20)) as msg, cast('green' as varchar(20)) as color from (select 1 as n) as t"
    "\nunion all\nselect 3 as id, cast('anyway' as varchar(20)) as msg, cast('purple' as varchar(20)) as color from (select 1 as n) as t",
)

_MERGE_EXCLUDE_MODEL_TD = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    merge_exclude_columns=['msg']
) }}

{% if not is_incremental() %}

select 1 as id, cast('hello' as varchar(20)) as msg, cast('blue' as varchar(20)) as color from (select 1 as n) as t
union all
select 2 as id, cast('goodbye' as varchar(20)) as msg, cast('red' as varchar(20)) as color from (select 1 as n) as t

{% else %}

select 1 as id, cast('hey' as varchar(20)) as msg, cast('blue' as varchar(20)) as color from (select 1 as n) as t
union all
select 2 as id, cast('yo' as varchar(20)) as msg, cast('green' as varchar(20)) as color from (select 1 as n) as t
union all
select 3 as id, cast('anyway' as varchar(20)) as msg, cast('purple' as varchar(20)) as color from (select 1 as n) as t

{% endif %}
"""


class TestIncrementalPredicatesTeradata(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def models(self):
        return {"delete_insert_incremental_predicates.sql": _PREDICATES_MODEL_TD}

    # Teradata does not support the `!=` operator (only `<>`); Error 3707.
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["id <> 2"],
                "+incremental_strategy": "delete+insert",
            }
        }


class TestIncrementalUniqueKeyTeradata(BaseIncrementalUniqueKey):
    # Same models() as SubBaseIncrementalUniqueKey, except the two "expected" table
    # fixtures are rewritten with `_teradata_safe_bare_union` (Error 3888: bare
    # literal UNION branches need a FROM clause on Teradata).
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "trinary_unique_key_list.sql": models__trinary_unique_key_list_sql,
            "nontyped_trinary_unique_key_list.sql": models__nontyped_trinary_unique_key_list_sql,
            "unary_unique_key_list.sql": models__unary_unique_key_list_sql,
            "not_found_unique_key.sql": models__not_found_unique_key_sql,
            "empty_unique_key_list.sql": models__empty_unique_key_list_sql,
            "no_unique_key.sql": models__no_unique_key_sql,
            "empty_str_unique_key.sql": models__empty_str_unique_key_sql,
            "str_unique_key.sql": models__str_unique_key_sql,
            "duplicated_unary_unique_key_list.sql": models__duplicated_unary_unique_key_list_sql,
            "not_found_unique_key_list.sql": models__not_found_unique_key_list_sql,
            "expected": {
                "one_str__overwrite.sql": _teradata_safe_bare_union(
                    models__expected__one_str__overwrite_sql
                ),
                "unique_key_list__inplace_overwrite.sql": _teradata_safe_bare_union(
                    models__expected__unique_key_list__inplace_overwrite_sql
                ),
            },
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Default Teradata incremental strategy is "append" which ignores unique_key.
        # Use delete+insert so the suite's unique-key assertions hold. Scope this to
        # only the models that actually declare a (non-empty) unique_key: applying it
        # project-wide would also force delete+insert on `empty_unique_key_list` and
        # `no_unique_key`, whose semantics require the default "append" behavior
        # (delete+insert with an empty/absent unique_key deletes the whole target).
        strategy_models = [
            "trinary_unique_key_list",
            "nontyped_trinary_unique_key_list",
            "unary_unique_key_list",
            "not_found_unique_key",
            "empty_str_unique_key",
            "str_unique_key",
            "duplicated_unary_unique_key_list",
            "not_found_unique_key_list",
        ]
        return {
            "models": {
                "test": {
                    model_name: {"+incremental_strategy": "delete+insert"}
                    for model_name in strategy_models
                }
            }
        }

    def test__empty_unique_key_list(self, project):
        expected_fields = self.get_expected_fields(relation="seed", seed_rows=9)
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="empty_unique_key_list",
            update_sql_file="add_new_rows",
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)


class TestMergeExcludeColumnsTeradata(BaseMergeExcludeColumns):
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_exclude_columns.sql": _MERGE_EXCLUDE_MODEL_TD}
