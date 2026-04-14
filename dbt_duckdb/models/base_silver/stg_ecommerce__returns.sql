{{ config(
    materialized='external',
    location=var('silver_base_path') ~ '/returns',
    options={'format': 'parquet', 'partition_by': 'ingestion_dt', 'overwrite': true}
) }}

select
    return_id,
    order_id,
    customer_id,
    email,
    return_date,
    reason,
    return_type,
    refunded_amount,
    return_channel,
    agent_id,
    refund_method,
    batch_id,
    ingestion_ts,
    ingestion_dt,
    event_id,
    source_file,
    return_dt
from {{ ref('int_returns_scored') }}
where is_valid
