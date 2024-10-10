{%- macro generate_natural_key(natural_key_cols) -%}
    {# Build Natural key string #}
    {%- if natural_key_cols is string -%}
        {%- set natural_key_cols = [natural_key_cols] -%}
    {%- endif -%}

    {%- set natural_key_string = [] -%}
    {%- for column in natural_key_cols -%}
        {%- do natural_key_string.append("coalesce(trim(" ~ column ~ "), '')") -%}
    {%- endfor -%}
    {%- set natural_key_string = natural_key_string | join("||'|'||") -%}
    
    {% do return(natural_key_string) %}
{% endmacro %}

{%- macro generate_surrogate_key(key_table, source_table, domain, natural_key_cols) -%}

    {# Check if key_table is properly defined and passed as a string #}
    {% if key_table is not defined or key_table == '' %}
        {% do exceptions.raise_compiler_error("The key_table parameter is missing or empty. ") %}
    {% endif %}

    insert into {{ref(key_table)}}
    select
        rank() over(partition by domain_cd order by customer_nk) (integer) customer_key 
        ,nk (varchar(10000)) customer_nk
        ,domain_cd (char(100)) domain_cd
        ,current_timestamp created_ts
    from 
    (
        select {{generate_natural_key(natural_key_cols)}} nk
        , '{{domain}}' domain_cd 
        from {{ref(source_table)}}
        group by 1,2
    ) source
    where not exists
    (sel 1 from  {{ref(key_table)}} k where k.customer_nk=source.nk and k.domain_cd=source.domain_cd )

{% endmacro %}

{%- macro generate_surrogate_key_hook(surrogate_keys_dict) -%}
{% set surrogate_keys_hook = [] %}
{% for sk, params in surrogate_keys_dict.items() %}
  {% do surrogate_keys_hook.append("{{ generate_surrogate_key('"~params.get('key_table')~"', '"~params.get('source_table')~"', natural_key_cols="~params.get('natural_key_cols')~", domain='"~params.get('domain')~"') }}") %}
{% endfor %}
{% do return(surrogate_keys_hook) %}
{% endmacro %}

{%- macro list_natural_key_cols(surrogate_keys_dict) -%}
{# Loop through surrogate_keys and add each natural_key_cols to exclude_columns #}
{%- set exclude_columns = [] %}
{%- for sk, params in surrogate_keys_dict.items() %}
    {% if 'mask' in params and params.mask == True %}
        {%- for col in params.natural_key_cols %}
            {%- do exclude_columns.append(col) %}
        {%- endfor %}
    {%- endif %}
{%- endfor %}
{%- do return(exclude_columns) %}
{% endmacro %}