-- overriding the "get_expected_sql" and "get_fixture_sql" macros because the syntax used in default macros is not supported in Teradata
-- Default macros uses 'union all' in between multiple select statements without reference to any table or view which gives the below error
-- [Teradata Database] [Error 3888] A SELECT for a UNION,INTERSECT or MINUS must reference a table
-- To avoid this error we are are using a workaround [ from SYS_CALENDAR.CALENDAR where day_of_calendar = 1 ] in between those sel statements

{% macro get_expected_sql(rows, column_name_to_data_types) -%}
  {{ adapter.dispatch('get_expected_sql', 'dbt')(rows, column_name_to_data_types) }}
{%- endmacro %}

{% macro teradata__get_expected_sql(rows, column_name_to_data_types) %}

{%- if (rows | length) == 0 -%}
    select * from dbt_internal_unit_test_actual
    sample 0
{%- else -%}

{%- for row in rows -%}
{%- set formatted_row = format_row(row, column_name_to_data_types) -%}
select
{%- for column_name, column_value in formatted_row.items() %} {{ column_value }} as {{ column_name }}{% if not loop.last -%}, {% else %} {{ ' ' }} {%- endif %}
{%- endfor %}
{%- if not loop.last %}
from SYS_CALENDAR.CALENDAR where day_of_calendar = 1
union all
{% endif %}
{%- endfor -%}
from SYS_CALENDAR.CALENDAR where day_of_calendar = 1
{%- endif -%}

{% endmacro %}


{% macro get_fixture_sql(rows, column_name_to_data_types) -%}
  {{ adapter.dispatch('get_fixture_sql', 'dbt')(rows, column_name_to_data_types) }}
{%- endmacro %}


{% macro teradata__get_fixture_sql(rows, column_name_to_data_types) %}
-- Fixture for {{ model.name }}
{% set default_row = {} %}

{%- if not column_name_to_data_types -%}
{#-- Use defer_relation IFF it is available in the manifest and 'this' is missing from the database --#}
{%-   set this_or_defer_relation = defer_relation if (defer_relation and not load_relation(this)) else this -%}
{%-   set columns_in_relation = adapter.get_columns_in_relation(this_or_defer_relation) -%}

{%-   set column_name_to_data_types = {} -%}
{%-   for column in columns_in_relation -%}
{#-- This needs to be a case-insensitive comparison --#}
{%-     do column_name_to_data_types.update({column.name|lower: column.data_type}) -%}
{%-   endfor -%}
{%- endif -%}

{%- if not column_name_to_data_types -%}
    {{ exceptions.raise_compiler_error("Not able to get columns for unit test '" ~ model.name ~ "' from relation " ~ this) }}
{%- endif -%}

{%- for column_name, column_type in column_name_to_data_types.items() -%}
    {%- do default_row.update({column_name: (safe_cast("null", column_type) | trim )}) -%}
{%- endfor -%}

{%- for row in rows -%}
{%-   set formatted_row = format_row(row, column_name_to_data_types) -%}
{%-   set default_row_copy = default_row.copy() -%}
{%-   do default_row_copy.update(formatted_row) -%}
select
{%-   for column_name, column_value in default_row_copy.items() %} {{ column_value }} as {{ column_name }}{% if not loop.last -%}, {% else %} {{ ' ' }} {%- endif %}
{%-   endfor %}
{%-   if not loop.last %}
from SYS_CALENDAR.CALENDAR where day_of_calendar = 1
union all
{%    endif %}
{%- endfor -%}
from SYS_CALENDAR.CALENDAR where day_of_calendar = 1

{%- if (rows | length) == 0 -%}
    select
    {%- for column_name, column_value in default_row.items() %} {{ column_value }} as {{ column_name }}{% if not loop.last -%},{%- endif %}
    {%- endfor %}
    sample 0
{%- endif -%}
{% endmacro %}


-- We need to override "format_row" macro to remove the safe_cast() used in the default implementation.
-- We had to remove safe_cast because N/A was being picked as column_type in safe_casting, which was later running into issues

{%- macro format_row(row, column_name_to_data_types) -%}
    {#-- generate case-insensitive formatted row --#}
    {% set formatted_row = {} %}
    {%- for column_name, column_value in row.items() -%}
        {% set column_name = column_name|lower %}

        {%- if column_name not in column_name_to_data_types %}
            {#-- if user-provided row contains column name that relation does not contain, raise an error --#}
            {% set fixture_name = "expected output" if model.resource_type == 'unit_test' else ("'" ~ model.name ~ "'") %}
            {{ exceptions.raise_compiler_error(
                "Invalid column name: '" ~ column_name ~ "' in unit test fixture for " ~ fixture_name ~ "."
                "\nAccepted columns for " ~ fixture_name ~ " are: " ~ (column_name_to_data_types.keys()|list)
            ) }}
        {%- endif -%}

        {%- set column_type = column_name_to_data_types[column_name] %}

        {#-- sanitize column_value: wrap yaml strings in quotes, apply cast --#}
        {%- set column_value_clean = column_value -%}
        {%- if column_value is string -%}
            {%- set column_value_clean = dbt.string_literal(dbt.escape_single_quotes(column_value)) -%}
        {%- elif column_value is none -%}
            {%- set column_value_clean = 'null' -%}
        {%- endif -%}

        {%- set row_update = {column_name: column_value_clean} -%}
        {%- do formatted_row.update(row_update) -%}
    {%- endfor -%}
    {{ return(formatted_row) }}
{%- endmacro -%}



-- Overridden "get_unit_test_sql" macro to avoid right truncation of data
-- We are selecting "dbt_internal_unit_test_expected" first then doing union all with "dbt_internal_unit_test_actual"
-- So that "expected do not become "expect" in final result of unit tests

{% macro get_unit_test_sql(main_sql, expected_fixture_sql, expected_column_names) -%}
  {{ adapter.dispatch('get_unit_test_sql', 'dbt')(main_sql, expected_fixture_sql, expected_column_names) }}
{%- endmacro %}

{% macro teradata__get_unit_test_sql(main_sql, expected_fixture_sql, expected_column_names) -%}
-- Build actual result given inputs
with dbt_internal_unit_test_actual as (
  select
    {% for expected_column_name in expected_column_names %}{{expected_column_name}}{% if not loop.last -%},{% endif %}{%- endfor -%}, {{ dbt.string_literal("actual") }} as {{ adapter.quote("actual_or_expected") }}
  from (
    {{ main_sql }}
  ) _dbt_internal_unit_test_actual
),
-- Build expected result
dbt_internal_unit_test_expected as (
  select
    {% for expected_column_name in expected_column_names %}{{expected_column_name}}{% if not loop.last -%}, {% endif %}{%- endfor -%}, {{ dbt.string_literal("expected") }} as {{ adapter.quote("actual_or_expected") }}
  from (
    {{ expected_fixture_sql }}
  ) _dbt_internal_unit_test_expected
)
-- Union actual and expected results
select * from dbt_internal_unit_test_expected
union all
select * from dbt_internal_unit_test_actual
{%- endmacro %}