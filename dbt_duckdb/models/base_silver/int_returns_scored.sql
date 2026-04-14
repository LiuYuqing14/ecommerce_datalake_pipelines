{{ config(materialized='ephemeral') }}

{% set enforce_fk = strict_fk() %}

{#
STAGING CORE: returns (shared by base + quarantine)
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'returns') }}
    where {{ run_date_filter('ingest_dt') }}
),

dim_orders as (
    select distinct
        {{ normalize_string('order_id') }} as order_id
    from {{ source_parquet('bronze', 'orders') }}
    where {{ normalize_string('order_id') }} is not null
),

dim_customers as (
    select distinct
        customer_id
    from {{ dims_parquet('customers') }}
    where {{ run_date_filter('snapshot_dt') }}
),

cleaned as (
    select
        {{ normalize_string('return_id') }} as return_id,
        {{ normalize_string('order_id') }} as order_id,
        {{ normalize_string('customer_id') }} as customer_id,
        {{ safe_cast_timestamp('return_date') }} as return_date,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,
        {{ safe_cast_decimal('refunded_amount', 18, 2) }} as refunded_amount,
        {{ normalize_string_lower('email') }} as email,
        {{ normalize_string_lower('return_channel') }} as return_channel,
        {{ normalize_string_lower('refund_method') }} as refund_method,
        {{ normalize_string_lower('return_type') }} as return_type,
        {{ normalize_string('reason') }} as reason,
        {{ normalize_string('agent_id') }} as agent_id,
        {{ normalize_string('batch_id') }} as batch_id,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,
        {{ get_ingestion_dt() }} as ingestion_dt,
        coalesce(
            cast({{ safe_cast_timestamp('return_date') }} as date),
            cast({{ safe_cast_timestamp('ingestion_ts') }} as date)
        ) as return_dt
    from raw
),

validated as (
    select
        cleaned.*,
        dim_orders.order_id is not null as order_fk_valid,
        dim_customers.customer_id is not null as customer_fk_valid,
        row_number() over (
            partition by cleaned.return_id
            order by cleaned.ingestion_ts desc nulls last, cleaned.event_id desc
        ) as row_num
    from cleaned
    left join dim_orders
        on cleaned.order_id = dim_orders.order_id
    left join dim_customers
        on cleaned.customer_id = dim_customers.customer_id
),

scored as (
    select
        *,
        (
            {{ is_valid_id('return_id') }}
            and {{ is_valid_id('order_id') }}
            and {{ is_valid_id('customer_id') }}
            and {{ is_valid_timestamp('return_date') }}
            and (refunded_amount is null or refunded_amount >= 0)
            and (not {{ enforce_fk }} or (order_id is null or order_fk_valid))
            and (
                not {{ enforce_fk }}
                or customer_id is null
                or {{ is_guest_customer_id('customer_id') }}
                or customer_fk_valid
            )
            and row_num = 1
        ) as is_valid,
        coalesce(nullif(trim(concat_ws(' | ',
            case when not {{ is_valid_id('return_id') }} then 'missing_return_id' end,
            case when not {{ is_valid_id('order_id') }} then 'missing_order_id' end,
            case when not {{ is_valid_id('customer_id') }} then 'missing_customer_id' end,
            case when not {{ is_valid_timestamp('return_date') }} then 'invalid_return_date' end,
            case when refunded_amount < 0 then 'negative_refunded_amount' end,
            {% if enforce_fk %}
            case when order_id is not null and not order_fk_valid then 'order_fk_invalid' end,
            case
                when customer_id is not null
                and not {{ is_guest_customer_id('customer_id') }}
                and not customer_fk_valid
                then 'customer_fk_invalid'
            end,
            {% endif %}
            case when row_num > 1 then 'duplicate_return_id' end
        )), ''), 'all_fields_null') as invalid_reason
    from validated
)

select
    return_id,
    order_id,
    customer_id,
    email,
    return_date,
    reason,
    return_type,
    refunded_amount,
    return_channel,
    agent_id,
    refund_method,
    batch_id,
    ingestion_ts,
    ingestion_dt,
    event_id,
    source_file,
    return_dt,
    is_valid,
    invalid_reason,
    row_num
from scored
