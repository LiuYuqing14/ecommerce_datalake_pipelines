{{ config(materialized='ephemeral') }}

{% set enforce_fk = strict_fk() %}

{#
STAGING CORE: shopping_carts (shared by base + quarantine)
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'shopping_carts') }}
    where {{ run_date_filter('ingest_dt') }}
),

{% if enforce_fk %}
dim_customers as (
    select distinct
        customer_id
    from {{ dims_parquet('customers') }}
    where {{ run_date_filter('snapshot_dt') }}
),
{% endif %}

cleaned as (
    select
        {{ normalize_string('cart_id') }} as cart_id,
        {{ normalize_string('customer_id') }} as customer_id,
        {{ safe_cast_timestamp('created_at') }} as created_at,
        {{ safe_cast_timestamp('updated_at') }} as updated_at,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,
        {{ safe_cast_decimal('cart_total', 18, 2) }} as cart_total,
        {{ normalize_string_lower('status') }} as status,
        {{ normalize_string('batch_id') }} as batch_id,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,
        {{ get_ingestion_dt() }} as ingestion_dt,
        cast({{ safe_cast_timestamp('created_at') }} as date) as created_dt
    from raw
),

validated as (
    select
        cleaned.*,
        {% if enforce_fk %}
        dim_customers.customer_id is not null as customer_fk_valid,
        {% else %}
        true as customer_fk_valid,
        {% endif %}
        row_number() over (
            partition by cleaned.cart_id
            order by cleaned.ingestion_ts desc nulls last, cleaned.event_id desc
        ) as row_num
    from cleaned
    {% if enforce_fk %}
    left join dim_customers
        on cleaned.customer_id = dim_customers.customer_id
    {% endif %}
),

scored as (
    select
        *,
        (
            {{ is_valid_id('cart_id') }}
            and {{ is_valid_id('customer_id') }}
            and {{ is_valid_timestamp('created_at') }}
            and (cart_total is null or cart_total >= 0)
            and (updated_at is null or created_at is null or updated_at >= created_at)
            {% if enforce_fk %}
            and (
                customer_id is null
                or {{ is_guest_customer_id('customer_id') }}
                or customer_fk_valid
            )
            {% endif %}
            and row_num = 1
        ) as is_valid,
        coalesce(nullif(trim(concat_ws(' | ',
            case when not {{ is_valid_id('cart_id') }} then 'missing_cart_id' end,
            case when not {{ is_valid_id('customer_id') }} then 'missing_customer_id' end,
            case when not {{ is_valid_timestamp('created_at') }} then 'invalid_created_at' end,
            case when cart_total < 0 then 'negative_cart_total' end,
            case when updated_at < created_at and updated_at is not null and created_at is not null
                 then 'updated_before_created' end,
            {% if enforce_fk %}
            case
                when customer_id is not null
                and not {{ is_guest_customer_id('customer_id') }}
                and not customer_fk_valid
                then 'customer_fk_invalid'
            end,
            {% endif %}
            case when row_num > 1 then 'duplicate_cart_id' end
        )), ''), 'all_fields_null') as invalid_reason
    from validated
)

select
    cart_id,
    customer_id,
    created_at,
    updated_at,
    cart_total,
    status,
    batch_id,
    ingestion_ts,
    ingestion_dt,
    event_id,
    source_file,
    created_dt,
    is_valid,
    invalid_reason,
    row_num
from scored
