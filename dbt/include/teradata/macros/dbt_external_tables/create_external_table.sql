{% macro teradata__create_external_table(source_node) %}

    {# https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Definition-Language-Syntax-and-Examples/Table-Statements/CREATE-FOREIGN-TABLE/CREATE-FOREIGN-TABLE-Syntax #}
    
    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}

    CREATE FOREIGN TABLE {{source(source_node.source_name, source_node.name)}}
        {{ ', ' + external.tbl_properties if external.tbl_properties }}
        {% if columns -%}
        (
            {% for column in columns %}
                {%- set column_quoted = adapter.quote(column.name) if column.quote else column.name %}
                {{column_quoted}} {{column.data_type}} {{- ',' if not loop.last -}}
            {%- endfor %}
        )
        {%- endif %}
    USING (
        LOCATION('{{ external.location }}')
        {% if external.file_format -%} STOREDAS ('{{ external.file_format }}') {%- endif %}
        {% if external.row_format -%} ROWFORMAT ('{{ external.row_format }}') {%- endif %}
        {{ external.using }}
    )
    {% if partitions -%}
    PARTITION BY (
        {%- for partition in partitions -%}
            {{ partition.name }} {{ partition.data_type if partition.data_type }} {{', ' if not loop.last}}
        {%- endfor -%}
    )
    {%- endif %}
    
{% endmacro %}