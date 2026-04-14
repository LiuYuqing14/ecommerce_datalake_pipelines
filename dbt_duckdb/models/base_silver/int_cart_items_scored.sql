{{ config(materialized='ephemeral') }}

{% set enforce_fk = strict_fk() %}

{#
STAGING CORE: cart_items (shared by base + quarantine)
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'cart_items') }}
    where {{ run_date_filter('ingest_dt') }}
),

dim_shopping_carts as (
    select distinct
        {{ normalize_string('cart_id') }} as cart_id
    from {{ source_parquet('bronze', 'shopping_carts') }}
    where {{ normalize_string('cart_id') }} is not null
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
        {{ safe_cast_integer('cart_item_id') }} as cart_item_id,
        {{ normalize_string('cart_id') }} as cart_id,
        {{ safe_cast_integer('product_id') }} as product_id,
        {{ normalize_string('product_name') }} as product_name,
        {{ normalize_string_lower('category') }} as category,
        {{ safe_cast_timestamp('added_at') }} as added_at,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,
        {{ safe_cast_integer('quantity') }} as quantity,
        {{ safe_cast_decimal('unit_price', 18, 2) }} as unit_price,
        {{ normalize_string('batch_id') }} as batch_id,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,
        {{ get_ingestion_dt() }} as ingestion_dt,
        cast({{ safe_cast_timestamp('added_at') }} as date) as added_dt
    from raw
),

validated as (
    select
        cleaned.*,
        dim_shopping_carts.cart_id is not null as cart_fk_valid,
        dim_products.product_id is not null as product_fk_valid,
        row_number() over (
            partition by cleaned.cart_id, cleaned.product_id, cleaned.added_at
            order by cleaned.ingestion_ts desc nulls last, cleaned.event_id desc
        ) as row_num
    from cleaned
    left join dim_shopping_carts
        on cleaned.cart_id = dim_shopping_carts.cart_id
    left join dim_products
        on cleaned.product_id = dim_products.product_id
),

scored as (
    select
        *,
        (
            {{ is_positive_number('cart_item_id') }}
            and {{ is_valid_id('cart_id') }}
            and {{ is_positive_number('product_id') }}
            and {{ is_positive_number('quantity') }}
            and {{ is_non_negative_number('unit_price') }}
            and (not {{ enforce_fk }} or (cart_id is null or cart_fk_valid))
            and (not {{ enforce_fk }} or (product_id is null or product_fk_valid))
            and row_num = 1
        ) as is_valid,
        coalesce(nullif(trim(concat_ws(' | ',
            case when cart_item_id is null or cart_item_id <= 0 then 'invalid_cart_item_id' end,
            case when not {{ is_valid_id('cart_id') }} then 'missing_cart_id' end,
            case when product_id is null or product_id <= 0 then 'invalid_product_id' end,
            case when quantity is null or quantity <= 0 then 'invalid_quantity' end,
            case when unit_price is null then 'missing_unit_price' end,
            case when unit_price < 0 then 'negative_unit_price' end,
            {% if enforce_fk %}
            case when cart_id is not null and not cart_fk_valid then 'cart_fk_invalid' end,
            case when product_id is not null and not product_fk_valid then 'product_fk_invalid' end,
            {% endif %}
            case when row_num > 1 then 'duplicate_cart_item_line' end
        )), ''), 'all_fields_null') as invalid_reason
    from validated
)

select
    cart_item_id,
    cart_id,
    product_id,
    product_name,
    category,
    added_at,
    quantity,
    unit_price,
    batch_id,
    ingestion_ts,
    ingestion_dt,
    event_id,
    source_file,
    added_dt,
    is_valid,
    invalid_reason,
    row_num
from scored
