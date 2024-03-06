model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
          - type: check
            expression: id >= 1
        tests:
          - unique
      - name: color
        data_type: varchar(30)
      - name: date_day
        data_type: varchar(30)
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(30)
      - name: date_day
        data_type: varchar(30)
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(30)
      - name: date_day
        data_type: varchar(30)
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(30)
      - name: date_day
        data_type: varchar(30)
"""

my_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  1 as id,
  'blue' as color,
  '2019-01-01' as date_day
"""

my_model_with_nulls_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  -- null value for 'id'
  cast(null as {{ dbt.type_int() }}) as id,
  -- change the color as well (to test rollback)
  'red' as color,
  '2019-01-01' as date_day
"""


my_model_incremental_with_nulls_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'  )
}}

select
  -- null value for 'id'
  cast(null as {{ dbt.type_int() }}) as id,
  -- change the color as well (to test rollback)
  'red' as color,
  '2019-01-01' as date_day
"""


model_quoted_column_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
      materialized: table
    constraints:
      - type: check
        # this one is the on the user
        expression: ("from" = 'blue')
        columns: [ '"from"' ]
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
        tests:
          - unique
      - name: from  # reserved word
        quote: true
        data_type: varchar(30)
        constraints:
          - type: not_null
      - name: date_day
        data_type: varchar(30)
"""


my_incremental_model_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  1 as id,
  'blue' as color,
  '2019-01-01' as date_day
"""


# 'from' is a reserved word, so it must be quoted
my_model_with_quoted_column_name_sql = """
select
  'blue' as {{ adapter.quote('from') }},
  1 as id,
  '2019-01-01' as date_day
"""

constrained_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        expression: (id > 0)
      - type: check
        expression: id >= 1
      - type: primary_key
        columns: [ id ]
      - type: unique
        columns: [ color, date_day ]
        name: strange_uniqueness_requirement
      - type: foreign_key
        columns: [ id ]
        expression: {schema}.foreign_key_model (id)
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
        tests:
          - unique
      - name: color
        data_type: varchar(30)
        constraints:
          - type: not_null
      - name: date_day
        data_type: varchar(30)
        constraints:
          - type: not_null
  - name: foreign_key_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: primary_key
          - type: not_null
"""

foreign_key_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  1 as id
"""


# model columns in a different order to schema definitions
my_model_wrong_order_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""


my_model_wrong_name_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  'blue' as color,
  1 as err,
  '2019-01-01' as date_day
"""


my_model_data_type_sql = """
{{{{
  config(
    materialized = "table"
  )
}}}}

select
  {sql_value} as wrong_data_type_column_name
"""

model_data_type_schema_yml = """
version: 2
models:
  - name: my_model_data_type
    config:
      contract:
        enforced: true
    columns:
      - name: wrong_data_type_column_name
        data_type: {data_type}
"""

my_model_view_wrong_order_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""

my_model_view_wrong_name_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  'blue' as color,
  1 as err,
  '2019-01-01' as date_day
"""

my_model_incremental_wrong_order_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""

my_model_incremental_wrong_name_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  'blue' as color,
  1 as err,
  '2019-01-01' as date_day
"""


my_model_wrong_order_depends_on_fk_sql = """
{{
  config(
    materialized = "table"
  )
}}

-- depends_on: {{ ref('foreign_key_model') }}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""


model_fk_constraint_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
          - type: check
            expression: id >= 1
          - type: foreign_key
            expression: {schema}.foreign_key_model (id)
        tests:
          - unique
      - name: color
        data_type: varchar(30)
      - name: date_day
        data_type: varchar(30)
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(30)
      - name: date_day
        data_type: varchar(30)
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(30)
      - name: date_day
        data_type: varchar(30)
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(30)
      - name: date_day
        data_type: varchar(30)
  - name: foreign_key_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: primary_key
          - type: not_null
"""


my_model_incremental_wrong_order_depends_on_fk_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

-- depends_on: {{ ref('foreign_key_model') }}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""


create_table_macro_sql = """
{% macro create_table_macro() %}
create table numbers (n int not null primary key)
{% endmacro %}
"""