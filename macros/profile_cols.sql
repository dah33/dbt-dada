{% macro col_is_string(col) %}
    {{ return(col.is_string()) }}
{% endmacro %}

{% macro col_is_number(col) %}
    {{ return(col.is_number()) }}
{% endmacro %}

{% macro profile_strings(relation) %}
    {{ profile_cols(relation, col_is_string, n_empty=True, n_trailing=True, min_value=False, max_value=False, max_characters=True )}}
{% endmacro %}

{% macro profile_numbers(relation) %}
    {{ profile_cols(relation, col_is_number, avg_value=True )}}
{% endmacro %}

{#
--Output 0 or 1 iff input is 0 or 1, otherwise
--show the number with required precision in range (0,1).
--
--Same as TRUNC(number, precision) but anything slightly 
--above 0 maps to power(0.1, precision).
#}
{% macro rate_with_precision(precision) %}
    case when ({{ caller() }}) = 0 then 0.0
    else 
        greatest(1.0, 
            trunc(
                {{- caller() -}}
                * power(10.0, {{precision}}) / count(*)
            ) 
        ) / power(10.0, {{precision}})
    end
{% endmacro %}

{%- macro profile_cols(
    relation,
    cols=None,
    rate_precision=4,
    data_type=True,
    n_null=True, null_rate=True,
    n_unique=True, unique_rate=True,
    info_rate=False,
    n_empty=False, n_trailing=False,
    min_value=True, max_value=True, avg_value=False,
    max_characters=False,
    most_common_values=True
    ) %}

    {% set all_cols = adapter.get_columns_in_relation(relation) %}

    {% set use_cols = [] %}
    {% for col in all_cols if cols is none 
        or (cols is sequence and col.name is in(cols)) 
        or (cols is callable and cols(col)) %}
        {{ use_cols.append(col) or "" }}
    {% endfor %}

    {% if use_cols|length == 0 %}
        {% set msg = 'No columns match the argument `cols`' %}
        {{ exceptions.warn(msg) }}
        select '{{ msg }}' as error
    {% endif %}

    {%- for col in use_cols %}
    
        select

            '{{ col.name }}' as column_name

        {%- if data_type %},
            '{{ col.data_type }}' as data_type
        {% endif %}

        {%- if n_null %},
            count(case when {{ adapter.quote(col.name) }} is null then 1 end) as n_null
        {% endif %}

        {%- if null_rate %},
            {#--Not working, due to recompilation bugs: workaround is to 
              --delete the file target/partial_parse.msgpack, but this is a pain.
            {% call rate_with_precision(rate_precision) %}
                count(case when {{ adapter.quote(col.name) }} is null then 1 end)
            {% endcall %} 
            #} 
            case when (
                count(case when {{ adapter.quote(col.name) }} is null then 1 end)
            ) = 0 then 0.0
            else greatest(1.0, 
                trunc(
                    count(case when {{ adapter.quote(col.name) }} is null then 1 end)
                    * power(10.0, {{ rate_precision }}) / count(*)
                ) 
            ) / power(10.0, {{ rate_precision }})
            end as null_rate
        {% endif %}

        {%- if n_unique %},
            count(distinct {{ adapter.quote(col.name) }} ) as n_unique
        {% endif %}

        {%- if unique_rate %},
            {# 
            {% call rate_with_precision(rate_precision) %}
                count(distinct {{ adapter.quote(col.name) }})
            {% endcall %}
            #} 
            case when (
                count(distinct {{ adapter.quote(col.name) }} )
            ) = 0 then 0.0
            else greatest(1.0, 
                trunc(
                    count(distinct {{ adapter.quote(col.name) }} )
                    * power(10.0, {{ rate_precision }}) / count(*)
                ) 
            ) / power(10.0, {{ rate_precision }})
            end as unique_rate
        {% endif %}

        {%- if info_rate %},
            (
                with freq as (
                    select count(*) as f
                    from {{ relation }}
                    group by {{ adapter.quote(col.name) }}
                ),
                n as (
                    select sum(f) as n from freq
                )
                select -sum(f/n*ln(f/n)/ln(n)) from freq, n
            ) as info_rate
        {% endif %}

        {%- if n_empty %},
            {%- if col.is_string() %}
            count(case when {{ adapter.quote(col.name) }} = '' then 1 end)
            {% else %}
            null::integer
            {% endif %} as n_empty
        {% endif %}

        {%- if n_trailing %},
            {%- if col.is_string() %}
            count(case when {{ adapter.quote(col.name) }} != rtrim({{ adapter.quote(col.name) }}) then 1 end)
            {% else %}
            null::integer
            {% endif %} as n_trailing
        {% endif %}

        {#
        -- There is no min/max for boolean on PostgreSQL, so this converts them to 
        -- a string ('true' or 'false') before ordering, however it also converts
        -- dates to a string first, which is probably OK, but not ideal. Need
        -- to enhance dbt's Column class with is_date and is_boolean properties.
        #}
        {%- if min_value %},
            {%- if col.is_string() %}
            null::varchar
            {% elif col.is_number() %}
            cast(min({{ adapter.quote(col.name) }}) as varchar)
            {% else %}
            min(cast({{ adapter.quote(col.name) }} as varchar))
            {% endif %} as min_value
        {% endif %}

        {%- if max_value %},
            {%- if col.is_string() %}
            null::varchar
            {% elif col.is_number() %}
            cast(max({{ adapter.quote(col.name) }}) as varchar)
            {% else %}
            max(cast({{ adapter.quote(col.name) }} as varchar))
            {% endif %} as max_value
        {% endif %}

        {%- if avg_value %},
            {%- if col.is_number() %}
            cast(avg({{ adapter.quote(col.name) }}) as float)
            {% else %}
            null::float
            {% endif %} as avg_value
        {% endif %}

        {%- if max_characters %},
            {%- if col.is_string() %}
            max(char_length({{ adapter.quote(col.name) }}))
            {% else %}
            null::integer
            {% endif %} as max_characters
        {% endif %}

        {%- if most_common_values %},
            array_to_string(
                array(
                    with freq_table as (
                        select
                            coalesce(
                                {% if col.is_string() %}'"' || {% endif -%}
                                cast({{ adapter.quote(col.name) }} as varchar)
                                {%- if col.is_string() %} || '"'{% endif %},
                                'NULL'
                            ) as val, 
                            count(*) as freq
                        from {{ relation }}
                        group by 1
                        order by count(*) desc
                        limit 5
                    )
                    select val || ' (' || freq || ')' from freq_table
                ), 
            ', ') as most_common_values
        {% endif %}

        from {{ relation }}

        {% if not loop.last -%} union all {%- endif -%}
    
    {% endfor %}

{% endmacro %}