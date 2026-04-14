{{ config(materialized='ephemeral') }}

{% set enforce_fk = strict_fk() %}

{#
STAGING CORE: return_items (shared by base + quarantine)
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'return_items') }}
    where {{ run_date_filter('ingest_dt') }}
),

dim_returns as (
    select distinct
        {{ normalize_string('return_id') }} as return_id,
        cast({{ safe_cast_timestamp('return_date') }} as date) as return_dt
    from {{ source_parquet('bronze', 'returns') }}
    where {{ normalize_string('return_id') }} is not null
),

dim_orders as (
    select distinct
        {{ normalize_string('order_id') }} as order_id
    from {{ source_parquet('bronze', 'orders') }}
    where {{ normalize_string('order_id') }} is not null
),

dim_products as (
    select distinct
        {{ safe_cast_integer('product_id') }} as product_id
    from {{ dims_parquet('product_catalog') }}
    where {{ run_date_filter('snapshot_dt') }}
      and {{ safe_cast_integer('product_id') }} is not null
),

cleaned as (
    select
        {{ safe_cast_integer('return_item_id') }} as return_item_id,
        {{ normalize_string('return_id') }} as return_id,
        {{ normalize_string('order_id') }} as order_id,
        {{ safe_cast_integer('product_id') }} as product_id,
        {{ normalize_string('product_name') }} as product_name,
        {{ normalize_string_lower('category') }} as category,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,
        {{ safe_cast_integer('quantity_returned') }} as quantity_returned,
        {{ safe_cast_decimal('unit_price', 18, 2) }} as unit_price,
        {{ safe_cast_decimal('cost_price', 18, 2) }} as cost_price,
        {{ safe_cast_decimal('refunded_amount', 18, 2) }} as refunded_amount,
        {{ normalize_string('batch_id') }} as batch_id,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,
        {{ get_ingestion_dt() }} as ingestion_dt
    from raw
),

validated as (
    select
        cleaned.*,
        dim_returns.return_id is not null as return_fk_valid,
        dim_orders.order_id is not null as order_fk_valid,
        dim_products.product_id is not null as product_fk_valid,
        coalesce(dim_returns.return_dt, cast(cleaned.ingestion_ts as date)) as return_dt,
        row_number() over (
            partition by cleaned.return_id, cleaned.product_id
            order by cleaned.ingestion_ts desc nulls last, cleaned.event_id desc
        ) as row_num
    from cleaned
    left join dim_returns
        on cleaned.return_id = dim_returns.return_id
    left join dim_orders
        on cleaned.order_id = dim_orders.order_id
    left join dim_products
        on cleaned.product_id = dim_products.product_id
),

scored as (
    select
        *,
        (
            {{ is_positive_number('return_item_id') }}
            and {{ is_valid_id('return_id') }}
            and {{ is_valid_id('order_id') }}
            and {{ is_positive_number('product_id') }}
            and {{ is_positive_number('quantity_returned') }}
            and (unit_price is null or unit_price >= 0)
            and (cost_price is null or cost_price >= 0)
            and (refunded_amount is null or refunded_amount >= 0)
            and (not {{ enforce_fk }} or (return_id is null or return_fk_valid))
            and (not {{ enforce_fk }} or (order_id is null or order_fk_valid))
            and (not {{ enforce_fk }} or (product_id is null or product_fk_valid))
            and row_num = 1
        ) as is_valid,
        coalesce(nullif(trim(concat_ws(' | ',
            case when return_item_id is null or return_item_id <= 0 then 'invalid_return_item_id' end,
            case when not {{ is_valid_id('return_id') }} then 'missing_return_id' end,
            case when not {{ is_valid_id('order_id') }} then 'missing_order_id' end,
            case when product_id is null or product_id <= 0 then 'invalid_product_id' end,
            case when quantity_returned is null or quantity_returned <= 0 then 'invalid_quantity_returned' end,
            case when unit_price < 0 then 'negative_unit_price' end,
            case when cost_price < 0 then 'negative_cost_price' end,
            case when refunded_amount < 0 then 'negative_refunded_amount' end,
            {% if enforce_fk %}
            case when return_id is not null and not return_fk_valid then 'return_fk_invalid' end,
            case when order_id is not null and not order_fk_valid then 'order_fk_invalid' end,
            case when product_id is not null and not product_fk_valid then 'product_fk_invalid' end,
            {% endif %}
            case when row_num > 1 then 'duplicate_return_item_line' end
        )), ''), 'all_fields_null') as invalid_reason
    from validated
)

select
    return_item_id,
    return_id,
    order_id,
    product_id,
    product_name,
    category,
    quantity_returned,
    unit_price,
    cost_price,
    refunded_amount,
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
