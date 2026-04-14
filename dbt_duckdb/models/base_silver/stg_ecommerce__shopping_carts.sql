{{ config(
    materialized='external',
    location=var('silver_base_path') ~ '/shopping_carts',
    options={'format': 'parquet', 'partition_by': 'ingestion_dt', 'overwrite': true}
) }}

select
    cart_id,
    customer_id,
    created_at,
    updated_at,
    cart_total,
    status,
    batch_id,
    ingestion_ts,
    ingestion_dt,
    event_id,
    source_file,
    created_dt
from {{ ref('int_shopping_carts_scored') }}
where is_valid
