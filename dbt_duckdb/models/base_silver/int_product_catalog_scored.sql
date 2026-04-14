{{ config(materialized='ephemeral') }}

{#
STAGING CORE: product_catalog (shared by base + quarantine)
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'product_catalog', partition_key='category') }}
    where {{ run_date_filter('ingestion_ts') }}
),

cleaned as (
    select
        {{ safe_cast_integer('product_id') }} as product_id,
        {{ normalize_string('product_name') }} as product_name,
        {{ normalize_string_lower('category') }} as category,
        {{ safe_cast_decimal('unit_price', 18, 2) }} as unit_price,
        {{ safe_cast_decimal('cost_price', 18, 2) }} as cost_price,
        {{ safe_cast_integer('inventory_quantity') }} as inventory_quantity,
        {{ normalize_string('batch_id') }} as batch_id,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,
        {{ get_ingestion_dt('ingestion_ts') }} as ingestion_dt
    from raw
),

validated as (
    select
        cleaned.*,
        row_number() over (
            partition by cleaned.product_id
            order by cleaned.ingestion_ts desc nulls last, cleaned.event_id desc
        ) as row_num
    from cleaned
),

scored as (
    select
        *,
        (
            {{ is_positive_number('product_id') }}
            and {{ is_valid_id('product_name') }}
            and {{ is_non_negative_number('unit_price') }}
            and (cost_price is null or cost_price >= 0)
            and (inventory_quantity is null or inventory_quantity >= 0)
            and (unit_price is null or cost_price is null or unit_price >= cost_price)
            and row_num = 1
        ) as is_valid,
        coalesce(nullif(trim(concat_ws(' | ',
            case when product_id is null or product_id <= 0 then 'invalid_product_id' end,
            case when not {{ is_valid_id('product_name') }} then 'missing_product_name' end,
            case when unit_price is null then 'missing_unit_price' end,
            case when unit_price < 0 then 'negative_unit_price' end,
            case when cost_price < 0 then 'negative_cost_price' end,
            case when inventory_quantity < 0 then 'negative_inventory' end,
            case when unit_price < cost_price and unit_price is not null and cost_price is not null
                 then 'unit_price_below_cost' end,
            case when row_num > 1 then 'duplicate_product_id' end
        )), ''), 'all_fields_null') as invalid_reason
    from validated
)

select
    product_id,
    product_name,
    category,
    unit_price,
    cost_price,
    inventory_quantity,
    batch_id,
    ingestion_ts,
    event_id,
    source_file,
    ingestion_dt,
    is_valid,
    invalid_reason,
    row_num
from scored
