{{ config(
    materialized='external',
    location=var('silver_base_path') ~ '/customers',
    options={'format': 'parquet', 'partition_by': 'signup_dt', 'overwrite': true}
) }}

select
    customer_id,
    email,
    signup_date,
    first_name,
    last_name,
    phone_number,
    gender,
    age,
    is_guest,
    customer_status,
    signup_channel,
    loyalty_tier,
    initial_loyalty_tier,
    email_verified,
    marketing_opt_in,
    mailing_address,
    billing_address,
    loyalty_enrollment_date,
    clv_bucket,
    batch_id,
    ingestion_ts,
    ingestion_dt,
    event_id,
    source_file,
    signup_dt
from {{ ref('int_customers_scored') }}
where is_valid
