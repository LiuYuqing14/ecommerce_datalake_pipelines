{{ config(
    materialized='external',
    location=var('silver_base_path') ~ '/quarantine/cart_items',
    options={'format': 'parquet', 'partition_by': 'ingestion_dt', 'overwrite': true}
) }}

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
    invalid_reason,
    row_num
from {{ ref('int_cart_items_scored') }}
where not is_valid
