{{ config(
    materialized='external',
    location=var('silver_base_path') ~ '/quarantine/product_catalog',
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
    ingestion_dt,
    event_id,
    source_file,
    invalid_reason,
    row_num
from {{ ref('int_product_catalog_scored') }}
where not is_valid
