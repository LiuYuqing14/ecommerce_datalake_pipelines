{{ config(
    materialized='external',
    location=var('silver_base_path') ~ '/product_catalog',
    options={'format': 'parquet', 'partition_by': 'ingestion_dt', 'overwrite': true}
) }}

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
    ingestion_dt
from {{ ref('int_product_catalog_scored') }}
where is_valid
