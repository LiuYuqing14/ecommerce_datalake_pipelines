-- ============================================================================
-- String Cleaning Macros
-- ============================================================================

{% macro normalize_string(column_name) %}
    case
        when lower(trim({{ column_name }})) in ('', 'none', 'null', 'n/a', 'nan') then null
        else trim({{ column_name }})
    end
{%- endmacro %}

{% macro normalize_string_lower(column_name) %}
    case
        when lower(trim({{ column_name }})) in ('', 'none', 'null', 'n/a', 'nan') then null
        else lower(trim({{ column_name }}))
    end
{%- endmacro %}

-- ============================================================================
-- Type Casting Macros with Null Handling
-- ============================================================================

{% macro safe_cast_timestamp(column_name) %}
    try_cast(
        case
            when lower(trim({{ column_name }}::varchar)) in ('', 'none', 'null') then null
            else trim({{ column_name }}::varchar)
        end as timestamp
    )
{%- endmacro %}

{% macro safe_cast_date(column_name) %}
    try_cast(
        case
            when lower(trim({{ column_name }}::varchar)) in ('', 'none', 'null') then null
            else trim({{ column_name }}::varchar)
        end as date
    )
{%- endmacro %}

{% macro safe_cast_decimal(column_name, precision=18, scale=2) %}
    try_cast(
        case
            when lower(trim({{ column_name }}::varchar)) in ('', 'none', 'null') then null
            else {{ column_name }}
        end as decimal({{ precision }}, {{ scale }})
    )
{%- endmacro %}

{% macro safe_cast_integer(column_name) %}
    try_cast(
        case
            when lower(trim({{ column_name }}::varchar)) in ('', 'none', 'null') then null
            else {{ column_name }}
        end as bigint
    )
{%- endmacro %}

{% macro safe_cast_boolean(column_name) %}
    try_cast(
        case
            when lower(trim({{ column_name }}::varchar)) in ('', 'none', 'null') then null
            else trim({{ column_name }}::varchar)
        end as boolean
    )
{%- endmacro %}

-- ============================================================================
-- Validation Helper Macros
-- ============================================================================

{% macro is_valid_id(column_name) %}
    ({{ column_name }} is not null and {{ column_name }} != '')
{%- endmacro %}

{% macro is_valid_timestamp(column_name) %}
    ({{ column_name }} is not null)
{%- endmacro %}

{% macro is_valid_email(column_name) %}
    ({{ column_name }} is not null
     and {{ column_name }} like '%@%.%'
     and length({{ column_name }}) >= 5)
{%- endmacro %}

{% macro is_guest_customer_id(column_name) %}
    ({{ column_name }} is not null and {{ column_name }} like 'GUEST-%')
{%- endmacro %}

{% macro is_positive_number(column_name) %}
    ({{ column_name }} is not null and {{ column_name }} > 0)
{%- endmacro %}

{% macro is_non_negative_number(column_name) %}
    ({{ column_name }} is not null and {{ column_name }} >= 0)
{%- endmacro %}

{% macro get_ingestion_dt(ingest_dt_col='ingest_dt') %}
    cast({{ ingest_dt_col }} as date)
{%- endmacro %}

-- ============================================================================
-- Partition Window Filter
-- ============================================================================

{% macro run_date_filter(column_name) %}
    {%- set run_date = var('run_date', '') -%}
    {%- set lookback_days = var('lookback_days', 0) -%}
    {%- if run_date -%}
        ({{ column_name }} is not null and cast({{ column_name }} as date) between
         (date '{{ run_date }}' - interval '{{ lookback_days }}' day) and date '{{ run_date }}')
    {%- else -%}
        true
    {%- endif -%}
{%- endmacro %}

-- ============================================================================
-- Deduplication Window Function
-- ============================================================================

{% macro deduplicate_by(partition_cols, order_by_col, order_direction='desc') %}
    row_number() over (
        partition by {{ partition_cols | join(', ') }}
        order by {{ order_by_col }} {{ order_direction }}
    ) = 1
{%- endmacro %}

-- ============================================================================
-- Environment Flags
-- ============================================================================

{% macro strict_fk() %}
    {% set override = env_var('STRICT_FK', '') %}
    {% if override %}
        {% if override | lower in ['true', '1', 'yes'] %}
            {{ return(true) }}
        {% else %}
            {{ return(false) }}
        {% endif %}
    {% else %}
        {% if env_var('PIPELINE_ENV', 'local') | lower in ['prod'] %}
            {{ return(true) }}
        {% else %}
            {{ return(false) }}
        {% endif %}
    {% endif %}
{% endmacro %}
