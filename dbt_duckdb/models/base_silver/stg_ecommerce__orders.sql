{{ config(
    materialized='external',
    location=var('silver_base_path') ~ '/orders',
    options={'format': 'parquet', 'partition_by': 'ingestion_dt', 'overwrite': true}
) }}

select
    order_id,
    total_items,
    order_date,
    customer_id,
    email,
    order_channel,
    is_expedited,
    customer_tier,
    gross_total,
    net_total,
    total_discount_amount,
    payment_method,
    shipping_speed,
    shipping_cost,
    agent_id,
    actual_shipping_cost,
    payment_processing_fee,
    shipping_address,
    billing_address,
    clv_bucket,
    is_reactivated,
    batch_id,
    ingestion_ts,
    ingestion_dt,
    event_id,
    source_file,
    order_dt
from {{ ref('int_orders_scored') }}
where is_valid
