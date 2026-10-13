import os
import pytest
from dbt.tests.adapter.aliases.fixtures import MACROS__CAST_SQL, MACROS__EXPECT_VALUE_SQL
from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliases,
    BaseAliasErrors,
    BaseSameAliasDifferentSchemas,
)

# Teradata uses CAST() instead of ::text
_TERADATA_STRING_LITERAL = """
{% macro teradata__string_literal(s) %}
    cast('{{ s }}' as varchar(255))
{% endmacro %}
"""

# Teradata requires <> instead of != for not-equal comparisons
_TERADATA_EXPECT_VALUE = """
{% test expect_value(model, field, value) %}
select *
from {{ model }}
where {{ field }} <> '{{ value }}'
{% endtest %}
"""


def _single_thread_profile():
    return {
        'type': 'teradata',
        'threads': 1,
        'host': os.getenv('DBT_TERADATA_SERVER_NAME', 'localhost'),
        'user': os.getenv('DBT_TERADATA_USERNAME', 'dbc'),
        'password': os.getenv('DBT_TERADATA_PASSWORD', 'dbc'),
        'tmode': os.getenv('DBT_TERADATA_TMODE', 'ANSI'),
        'log': '0',
    }


class TestAliasesTeradata(BaseAliases):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return _single_thread_profile()

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": MACROS__CAST_SQL,
            "expect_value.sql": _TERADATA_EXPECT_VALUE,
            "teradata_cast.sql": _TERADATA_STRING_LITERAL,
        }


class TestAliasErrorsTeradata(BaseAliasErrors):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": MACROS__CAST_SQL,
            "expect_value.sql": _TERADATA_EXPECT_VALUE,
            "teradata_cast.sql": _TERADATA_STRING_LITERAL,
        }


class TestSameAliasDifferentSchemasTeradata(BaseSameAliasDifferentSchemas):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return _single_thread_profile()

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": MACROS__CAST_SQL,
            "expect_value.sql": _TERADATA_EXPECT_VALUE,
            "teradata_cast.sql": _TERADATA_STRING_LITERAL,
        }

# TestSameAliasDifferentDatabases omitted: Teradata treats database == schema,
# so a second database target would resolve identically.
