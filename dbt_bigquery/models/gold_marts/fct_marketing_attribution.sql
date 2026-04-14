with orders as (
  select
    ap.order_channel as channel,
    date(ap.order_date) as order_date,
    countif(ap.is_recovered) as recovered_orders,
    count(*) as total_orders
  from {{ source('silver', 'int_attributed_purchases') }} ap
  group by 1, 2
),
carts as (
  select
    coalesce(ca.order_channel, 'unknown') as channel,
    date(ca.cart_dt) as cart_date,
    countif(ca.cart_status = 'abandoned') as abandoned_carts,
    countif(ca.cart_status = 'converted') as converted_carts,
    avg(ca.time_to_purchase_hours) as avg_time_to_purchase_hours,
    sum(ca.abandoned_value) as abandoned_value
  from {{ source('silver', 'int_cart_attribution') }} ca
  group by 1, 2
),
retention as (
  select
    date(rs.ingest_dt) as ingest_date,
    countif(rs.is_in_danger_zone) as at_risk_customers,
    count(*) as total_customers
  from {{ source('silver', 'int_customer_retention_signals') }} rs
  group by 1
)
select
  coalesce(orders.order_date, carts.cart_date) as metric_date,
  coalesce(orders.channel, carts.channel) as channel,
  orders.recovered_orders,
  orders.total_orders,
  carts.abandoned_carts,
  carts.converted_carts,
  carts.abandoned_value,
  carts.avg_time_to_purchase_hours,
  retention.at_risk_customers,
  retention.total_customers
from orders
full outer join carts
  on orders.order_date = carts.cart_date
  and orders.channel = carts.channel
left join retention
  on retention.ingest_date = coalesce(orders.order_date, carts.cart_date)
