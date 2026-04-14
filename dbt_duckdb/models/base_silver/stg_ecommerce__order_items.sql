{{ config(
    materialized='external',
    location=var('silver_base_path') ~ '/order_items',
    options={'format': 'parquet', 'partition_by': 'ingestion_dt', 'overwrite': true}
) }}

select
    order_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    discount_amount,
    cost_price,
    batch_id,
    ingestion_ts,
    ingestion_dt,
    event_id,
    source_file,
    order_dt
from {{ ref('int_order_items_scored') }}
where is_valid
