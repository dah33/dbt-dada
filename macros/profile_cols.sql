{% macro profile_cols(
    relation,
    n_null=True, null_frac=False,
    n_unique=True, empty_frac=False,
    n_trailing=True, trailing_frac=False,
    n_non_ascii=True, non_ascii_frac=False,
    min_length=True, max_length=False, avg_length=False,
    most_common=True,
    first_five=True,
    ) %}

with results as (

    {% set cols = adapter.get_columns_in_relation(relation) %}

    {%- for col in cols %}
    
    select

        '{{ col.name }}' as column_name,
        '{{ col.data_type }}' as column_type

    {% if n_null -%},
        count(case when {{ adapter.quote(col.name) }}  is null then 1 end) as n_null
    {% endif %}

    {%- if null_frac %},
        trunc(
            10000.0 * 
            count(case when {{ adapter.quote(col.name) }}  is null then 1 end)
            / count(*)
        ) / 10000.0 as null_frac
    {% endif %}

    {%- if n_unique %},
        count(distinct {{ adapter.quote(col.name) }} ) as n_unique
    {% endif %}

    {%- if unique_frac %},
        trunc(
            10000.0 * 
            count(distinct {{ adapter.quote(col.name) }})
            / count(*)
        ) / 10000.0 as unique_frac
    {% endif %}

    {%- if n_empty %},
        {%- if col.is_string() %}
        count(case when {{ adapter.quote(col.name) }} = '' then 1 end)
        {% else %}
        null::integer
        {%- endif %} as n_empty
    {% endif %}

    {%- if empty_frac %},
        {%- if col.is_string() %}
        trunc(
            10000.0 * 
            count(case when {{ adapter.quote(col.name) }} = '' then 1 end)
            / count(*)
        ) / 10000.0
        {% else %}
        null::integer
        {%- endif %} as empty_frac
    {% endif %}

    --count(case when nullif({{ adapter.quote(col.name) }} ,'') ~ '[^[:ascii:]]' then 1 end)

    {%- if most_common %},
    (  
        select cast({{ adapter.quote(col.name) }} as varchar) 
        from {{ relation }} 
        where {{ adapter.quote(col.name) }} is not null 
        group by {{ adapter.quote(col.name) }} 
        order by count(*) desc 
        limit 1
    ) as most_common, {# TODO: display nulls #}
    {% endif %}

    {%- if col.is_number() %} {# are dates numbers? #}
        cast(min({{ adapter.quote(col.name) }}) as varchar) as min_value,
        cast(max({{ adapter.quote(col.name) }}) as varchar) as max_value,
    {% else %}
        min(cast({{ adapter.quote(col.name) }} as varchar)) as min_value,
        max(cast({{ adapter.quote(col.name) }} as varchar)) as max_value,
    {% endif %}

    {%- if first_five %},
        array_to_string(array(select cast({{ adapter.quote(col.name) }} as varchar) from {{ relation }} group by {{ adapter.quote(col.name) }} limit 5), ', ') as first_five --TODO: quote strings, so empty strings shown clearly, what happens with NULLs?
    {% endif %}

    from {{ relation }}

    {% if not loop.last -%} union all {%- endif -%}
    
    {% endfor %}
)
select * from results 