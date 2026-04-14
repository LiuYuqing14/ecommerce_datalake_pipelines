{{ config(
    materialized='table'
) }}

with raw as (
    select *
    from {{ source_parquet('bronze', 'orders') }}
),
customers as (
    select distinct
        case
            when lower(trim(customer_id)) in ('', 'none', 'null') then null
            else trim(customer_id)
        end as customer_id
    from {{ source_parquet('bronze', 'customers') }}
),
cleaned as (
    select
        case
            when lower(trim(order_id)) in ('', 'none', 'null') then null
            else trim(order_id)
        end as order_id,
        try_cast(total_items as bigint) as total_items,
        try_cast(
            case
                when lower(trim(order_date)) in ('', 'none', 'null') then null
                else trim(order_date)
            end as timestamp
        ) as order_date,
        case
            when lower(trim(customer_id)) in ('', 'none', 'null') then null
            else trim(customer_id)
        end as customer_id,
        case
            when lower(trim(email)) in ('', 'none', 'null') then null
            else lower(trim(email))
        end as email,
        case
            when lower(trim(order_channel)) in ('', 'none', 'null') then null
            else lower(trim(order_channel))
        end as order_channel,
        try_cast(
            case
                when lower(trim(is_expedited::varchar)) in ('', 'none', 'null') then null
                else trim(is_expedited::varchar)
            end as boolean
        ) as is_expedited,
        case
            when lower(trim(customer_tier)) in ('', 'none', 'null') then null
            else lower(trim(customer_tier))
        end as customer_tier,
        try_cast(gross_total as double) as gross_total,
        try_cast(net_total as double) as net_total,
        try_cast(total_discount_amount as double) as total_discount_amount,
        case
            when lower(trim(payment_method)) in ('', 'none', 'null') then null
            else lower(trim(payment_method))
        end as payment_method,
        case
            when lower(trim(shipping_speed)) in ('', 'none', 'null') then null
            else lower(trim(shipping_speed))
        end as shipping_speed,
        try_cast(shipping_cost as double) as shipping_cost,
        case
            when lower(trim(agent_id)) in ('', 'none', 'null') then null
            else trim(agent_id)
        end as agent_id,
        try_cast(actual_shipping_cost as double) as actual_shipping_cost,
        try_cast(payment_processing_fee as double) as payment_processing_fee,
        case
            when lower(trim(shipping_address)) in ('', 'none', 'null') then null
            else trim(shipping_address)
        end as shipping_address,
        case
            when lower(trim(billing_address)) in ('', 'none', 'null') then null
            else trim(billing_address)
        end as billing_address,
        case
            when lower(trim(clv_bucket)) in ('', 'none', 'null') then null
            else lower(trim(clv_bucket))
        end as clv_bucket,
        try_cast(
            case
                when lower(trim(is_reactivated::varchar)) in ('', 'none', 'null') then null
                else trim(is_reactivated::varchar)
            end as boolean
        ) as is_reactivated,
        case
            when lower(trim(batch_id)) in ('', 'none', 'null') then null
            else trim(batch_id)
        end as batch_id,
        try_cast(
            case
                when lower(trim(ingestion_ts)) in ('', 'none', 'null') then null
                else trim(ingestion_ts)
            end as timestamp
        ) as ingestion_ts,
        case
            when lower(trim(event_id)) in ('', 'none', 'null') then null
            else trim(event_id)
        end as event_id,
        case
            when lower(trim(source_file)) in ('', 'none', 'null') then null
            else trim(source_file)
        end as source_file,
        cast(
            try_cast(
                case
                    when lower(trim(order_date)) in ('', 'none', 'null') then null
                    else trim(order_date)
                end as timestamp
            ) as date
        ) as order_dt
    from raw
),
validated as (
    select
        cleaned.*,
        customers.customer_id is not null as customer_exists,
        row_number() over (
            partition by cleaned.order_id
            order by cleaned.ingestion_ts desc nulls last
        ) as row_num
    from cleaned
    left join customers
        on cleaned.customer_id = customers.customer_id
),
scored as (
    select
        *,
        (
            order_id is not null
            and customer_id is not null
            and order_date is not null
            and gross_total is not null
            and net_total is not null
            and gross_total >= 0
            and net_total >= 0
            and (total_discount_amount is null or total_discount_amount >= 0)
            and (gross_total is null or net_total is null or net_total <= gross_total)
            and (customer_id is null or customer_exists)
            and row_num = 1
        ) as is_valid,
        concat_ws(
            '|',
            case when order_id is null then 'missing_order_id' end,
            case when customer_id is null then 'missing_customer_id' end,
            case when order_date is null then 'invalid_order_date' end,
            case when gross_total is null then 'missing_gross_total' end,
            case when net_total is null then 'missing_net_total' end,
            case when gross_total < 0 then 'negative_gross_total' end,
            case when net_total < 0 then 'negative_net_total' end,
            case when total_discount_amount < 0 then 'negative_total_discount' end,
            case
                when gross_total is not null
                    and net_total is not null
                    and net_total > gross_total
                then 'net_gt_gross'
            end,
            case
                when customer_id is not null and not customer_exists
                then 'customer_fk_missing'
            end,
            case when row_num > 1 then 'duplicate_order_id' end
        ) as invalid_reason
    from validated
)
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
    event_id,
    source_file,
    order_dt
from scored
where is_valid
