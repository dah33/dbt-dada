{% macro profile_cols(
    relation,
    data_type = True,
    n_null=True, null_rate=True,
    n_unique=True, unique_rate=True,
    n_empty=True, empty_rate=False,
    n_trailing=True, trailing_rate=False,
    min_value=True, max_value=True, avg_value=True,
    min_length=False, max_length=False, 
    most_common=True,
    top_five=False
    ) %}

    with results as (

        {% set cols = adapter.get_columns_in_relation(relation) %}

        {%- for col in cols %}
        
        select

            '{{ col.name }}' as column_name

        {% if data_type -%},
            '{{ col.data_type }}' as data_type
        {% endif %}

        {%- if n_null %},
            count(case when {{ adapter.quote(col.name) }} is null then 1 end) as n_null
        {% endif %}

        {%- if null_rate %},
            case when count(case when {{ adapter.quote(col.name) }} is null then 1 end) = 0 then 0.0
            else greatest(0.0001, trunc(
                    10000.0 * 
                    count(case when {{ adapter.quote(col.name) }} is null then 1 end)
                    / count(*)
                ) / 10000.0) 
            end as null_rate
        {% endif %}

        {%- if n_unique %},
            count(distinct {{ adapter.quote(col.name) }} ) as n_unique
        {% endif %}

        {%- if unique_rate %},
            case when count(distinct {{ adapter.quote(col.name) }}) = 0 then 0.0
            else greatest(0.0001, trunc(
                    10000.0 * 
                    count(distinct {{ adapter.quote(col.name) }})
                    / count(*)
                ) / 10000.0) 
            end as unique_rate
        {% endif %}

        {%- if n_empty %},
            {%- if col.is_string() %}
            count(case when {{ adapter.quote(col.name) }} = '' then 1 end)
            {% else %}
            null::integer
            {%- endif %} as n_empty
        {% endif %}

        {%- if empty_rate %},
            {%- if col.is_string() %}
            trunc(
                10000.0 * 
                count(case when {{ adapter.quote(col.name) }} = '' then 1 end)
                / count(*)
            ) / 10000.0
            {% else %}
            null::integer
            {%- endif %} as empty_rate
        {% endif %}

        {%- if n_trailing %},
            {%- if col.is_string() %}
            count(case when {{ adapter.quote(col.name) }} != rtrim({{ adapter.quote(col.name) }}) then 1 end)
            {% else %}
            null::integer
            {%- endif %} as n_trailing
        {% endif %}

        {%- if trailing_rate %},
            {%- if col.is_string() %}
            trunc(
                10000.0 * 
                count(case when {{ adapter.quote(col.name) }} != rtrim({{ adapter.quote(col.name) }}) then 1 end)
                / count(*)
            ) / 10000.0
            {% else %}
            null::integer
            {%- endif %} as trailing_rate
        {% endif %}

        {#
        -- There is no min/max for boolean on PostgreSQL, so this converts them to 
        -- a string ('true' or 'false') before ordering, however it also converts
        -- dates to a string first, which is probably OK, but not ideal. Need
        -- to enhance dbt's Column class with is_date and is_boolean properties.
        #}
        {%- if min_value %},
            {%- if col.is_number() %}
            cast(min({{ adapter.quote(col.name) }}) as varchar)
            {% else %}
                {% if col.is_string() %}'"' || {% endif -%}
            min(cast({{ adapter.quote(col.name) }} as varchar))
                {%- if col.is_string() %} || '"'{% endif %}
            {% endif %} as min_value
        {% endif %}

        {%- if max_value %},
            {%- if col.is_number() %}
            cast(max({{ adapter.quote(col.name) }}) as varchar)
            {% else %}
                {% if col.is_string() %}'"' || {% endif -%}
            max(cast({{ adapter.quote(col.name) }} as varchar))
                {%- if col.is_string() %} || '"'{% endif %}
            {% endif %} as max_value
        {% endif %}

        {%- if avg_value %},
            {%- if col.is_number() %}
            cast(avg({{ adapter.quote(col.name) }}) as float)
            {% else %}
            null::float
            {% endif %} as avg_value
        {% endif %}

        {%- if min_length %},
            {%- if col.is_string() %}
            min(char_length({{ adapter.quote(col.name) }}))
            {% else %}
            null::integer
            {% endif %} as min_length
        {% endif %}

        {%- if max_length %},
            {%- if col.is_string() %}
            max(char_length({{ adapter.quote(col.name) }}))
            {% else %}
            null::integer
            {% endif %} as max_length
        {% endif %}

        {%- if most_common %},
        (  
            select 
            {% if col.is_string() %}'"' || {% endif -%}
                cast({{ adapter.quote(col.name) }} as varchar) 
            {%- if col.is_string() %} || '"'{% endif %}
            from {{ relation }} 
            group by {{ adapter.quote(col.name) }} 
            order by count(*) desc 
            limit 1
        ) as most_common
        {% endif %}

        {%- if top_five %},
            array_to_string(
                array(
                    select
                    coalesce(
                        {% if col.is_string() %}'"' || {% endif -%}
                        cast({{ adapter.quote(col.name) }} as varchar)
                        {%- if col.is_string() %} || '"'{% endif %},
                        'NULL'
                    )
                    from {{ relation }}
                    group by 1 {# todo show nulls #}
                    order by count(*) desc
                    limit 5
                ), 
            ', ') as top_five
        {% endif %}

        from {{ relation }}

        {% if not loop.last -%} union all {%- endif -%}
        
        {% endfor %}
    )
    select * from results 

{% endmacro %}