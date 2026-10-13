import pytest

from dbt.tests.adapter.dbt_clone.fixtures import (
    custom_can_clone_tables_false_macros_sql,
    infinite_macros_sql,
    macros_sql,
)
from dbt.tests.adapter.dbt_clone.test_dbt_clone import (
    BaseCloneNotPossible,
    BaseCloneSameTargetAndState,
)

# Upstream's own get_schema_name.sql macro deliberately puts seeds in a
# separate "<schema>_seeds" schema (its comment: "put seeds into a separate
# schema in 'prod', to verify that cloning in 'dev' still works"), and
# table_model.sql then selects from that seed cross-schema when building
# ephemeral_model/view_model. Verified live that this specific cross-schema
# read requires SELECT WITH GRANT OPTION on the source schema -- a
# server-side permission not available on this Teradata instance (Error
# 5315) -- and fails during `run_and_save_state`'s `run` step, before either
# subclass's own assertions are ever reached.
#
# Rather than skip the whole file (losing all executed clone coverage), keep
# every model/seed in a single schema by overriding generate_schema_name to
# always return the default schema, sacrificing only the upstream
# cross-schema-seed scenario (which this Teradata instance can't grant
# permissions for anyway) while still exercising the real clone assertions
# ("can_clone_table() == False" / same state+target rejection).
_get_schema_name_single_schema_sql = """
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ return(target.schema) }}
{%- endmacro %}
"""


class _SingleSchemaCloneMixin:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
            "get_schema_name.sql": _get_schema_name_single_schema_sql,
        }


class TestCloneNotPossibleTeradata(_SingleSchemaCloneMixin, BaseCloneNotPossible):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
            "get_schema_name.sql": _get_schema_name_single_schema_sql,
            "my_can_clone_tables.sql": custom_can_clone_tables_false_macros_sql,
        }


class TestCloneSameTargetAndStateTeradata(_SingleSchemaCloneMixin, BaseCloneSameTargetAndState):
    pass

# BaseClonePossible and BaseCloneSameSourceAndTarget omitted:
# Teradata does not support CREATE TABLE ... AS CLONE OF.

