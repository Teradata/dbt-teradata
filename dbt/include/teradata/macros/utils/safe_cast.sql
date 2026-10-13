{% macro teradata__safe_cast(field, type) %}
    {%- set field_as_string =  dbt.string_literal(field) if field is number else field -%}
    {#- Teradata defaults the *unsized* spellings ("varchar" / "character
        varying") to VARCHAR(1) rather than rejecting them, so passing them
        through to TRYCAST as-is silently truncates to a single character:
        `SELECT TRYCAST('abc' AS VARCHAR)` returns 'a'. That failure is silent
        and data-losing, which is why these spellings must be widened here.

        An unsized type is exactly what reaches this macro for unit-test
        fixtures: dbt-adapters' `format_row` macro deliberately strips the
        declared length from string fixture values to avoid a different silent
        truncation (dbt-core GH-11974). Widen to Teradata's maximum VARCHAR
        width; VARCHAR(64001) is rejected outright, so 64000 is the ceiling.

        A length the caller *did* specify is always honoured and needs no
        special handling -- TRYCAST applies it correctly, truncating to the
        requested width per normal CAST semantics. Widening it would silently
        ignore an explicit request such as safe_cast(x, 'varchar(10)'). -#}
    {%- set type_lower = type | lower | trim -%}
    {%- if type_lower in ('varchar', 'character varying') -%}
    trycast({{field_as_string}} as varchar(64000))
    {%- else -%}
    trycast({{field_as_string}} as {{type}})
    {%- endif -%}
{% endmacro %}
