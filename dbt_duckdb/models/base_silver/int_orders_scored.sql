{{ config(materialized='ephemeral') }}

{% set enforce_fk = strict_fk() %}

{#
STAGING CORE: orders (shared by base + quarantine)
- Cleans, validates, and scores rows
- Returns is_valid + invalid_reason for downstream filtering
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'orders') }}
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
        {{ normalize_string('order_id') }} as order_id,
        {{ normalize_string('customer_id') }} as customer_id,
        {{ safe_cast_timestamp('order_date') }} as order_date,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,
        {{ safe_cast_integer('total_items') }} as total_items,
        {{ safe_cast_decimal('gross_total', 18, 2) }} as gross_total,
        {{ safe_cast_decimal('net_total', 18, 2) }} as net_total,
        {{ safe_cast_decimal('total_discount_amount', 18, 2) }} as total_discount_amount,
        {{ safe_cast_decimal('shipping_cost', 18, 2) }} as shipping_cost,
        {{ safe_cast_decimal('actual_shipping_cost', 18, 2) }} as actual_shipping_cost,
        {{ safe_cast_decimal('payment_processing_fee', 18, 2) }} as payment_processing_fee,
        {{ safe_cast_boolean('is_expedited') }} as is_expedited,
        {{ safe_cast_boolean('is_reactivated') }} as is_reactivated,
        {{ normalize_string_lower('email') }} as email,
        {{ normalize_string_lower('order_channel') }} as order_channel,
        {{ normalize_string_lower('customer_tier') }} as customer_tier,
        {{ normalize_string_lower('payment_method') }} as payment_method,
        {{ normalize_string_lower('shipping_speed') }} as shipping_speed,
        {{ normalize_string_lower('clv_bucket') }} as clv_bucket,
        {{ normalize_string('agent_id') }} as agent_id,
        {{ normalize_string('shipping_address') }} as shipping_address,
        {{ normalize_string('billing_address') }} as billing_address,
        {{ normalize_string('batch_id') }} as batch_id,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,
        {{ get_ingestion_dt() }} as ingestion_dt,
        coalesce(
            cast({{ safe_cast_timestamp('order_date') }} as date),
            cast({{ safe_cast_timestamp('ingestion_ts') }} as date)
        ) as order_dt
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
            partition by cleaned.order_id
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
            {{ is_valid_id('order_id') }}
            and {{ is_valid_id('customer_id') }}
            and {{ is_valid_timestamp('order_date') }}
            and {{ is_non_negative_number('gross_total') }}
            and {{ is_non_negative_number('net_total') }}
            and (total_discount_amount is null or total_discount_amount >= 0)
            and (net_total <= gross_total or gross_total is null or net_total is null)
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
            case when not {{ is_valid_id('order_id') }} then 'missing_order_id' end,
            case when not {{ is_valid_id('customer_id') }} then 'missing_customer_id' end,
            case when not {{ is_valid_timestamp('order_date') }} then 'invalid_order_date' end,
            case when gross_total is null then 'missing_gross_total' end,
            case when net_total is null then 'missing_net_total' end,
            case when gross_total < 0 then 'negative_gross_total' end,
            case when net_total < 0 then 'negative_net_total' end,
            case when total_discount_amount < 0 then 'negative_discount' end,
            case when net_total > gross_total and gross_total is not null and net_total is not null
                 then 'net_exceeds_gross' end,
            {% if enforce_fk %}
            case
                when customer_id is not null
                and not {{ is_guest_customer_id('customer_id') }}
                and not customer_fk_valid
                then 'customer_fk_invalid'
            end,
            {% endif %}
            case when row_num > 1 then 'duplicate_order_id' end
        )), ''), 'all_fields_null') as invalid_reason
    from validated
)

select
    order_id,
    total_items,
    order_date,
    customer_id,
    email,
    order_channel,
    is_expedited,
    customer_tier,
    gross_total,
    net_total,
    total_discount_amount,
    payment_method,
    shipping_speed,
    shipping_cost,
    agent_id,
    actual_shipping_cost,
    payment_processing_fee,
    shipping_address,
    billing_address,
    clv_bucket,
    is_reactivated,
    batch_id,
    ingestion_ts,
    ingestion_dt,
    event_id,
    source_file,
    order_dt,
    is_valid,
    invalid_reason,
    row_num
from scored
