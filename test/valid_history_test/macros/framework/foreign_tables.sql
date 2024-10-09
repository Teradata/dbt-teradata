
{% macro replace_foreign_table(schema_name, table_name, source_path) %}

{%- set relation = adapter.get_relation(schema=schema_name, database=schema_name, identifier=table_name) -%}

{%- if relation -%}
    {% do run_query('drop table '~schema_name~'.'~table_name~';') %}
{% endif -%}

{% do run_query('create foreign table '~schema_name~'.'~table_name~" using (location('"~source_path~"'));") %}

{% endmacro %}

{# This macro maps the dbt source definition to the foreign table creation macro
    - Its primary purpose is to translate source name to the actual schema name 
    - It could be extended to retrieve the source location from the source definition (eg. in schema.yml)
#}
{% macro source_object_storage(source_name, table_name, source_path) %}
    {% set source_config = source(source_name, table_name) %}
    {% set meta = source_config.meta %}
    
    {{ replace_foreign_table(
        source_config.schema,
        table_name,
        source_path,
    ) }}
{% endmacro %}
