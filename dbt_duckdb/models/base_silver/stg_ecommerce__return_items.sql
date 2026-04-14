{{ config(
    materialized='external',
    location=var('silver_base_path') ~ '/return_items',
    options={'format': 'parquet', 'partition_by': 'ingestion_dt', 'overwrite': true}
) }}

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
    return_dt
from {{ ref('int_return_items_scored') }}
where is_valid
