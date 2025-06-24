{%- macro teradata__create_external_schema(source_node) -%}
    {{ exceptions.raise_compiler_error(
        "Creating external schema is not implemented for the Teradata adapter"
    ) }}
{%- endmacro -%}