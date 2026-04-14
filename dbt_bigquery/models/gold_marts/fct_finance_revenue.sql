with finance as (
  select
    date(rf.order_dt) as order_date,
    rf.order_channel as order_channel,
    sum(rf.gross_total) as gross_revenue,
    sum(rf.net_total) as net_revenue
  from {{ source('silver', 'int_regional_financials') }} rf
  group by 1, 2
),
shipping as (
  select
    date(se.order_dt) as order_date,
    se.order_channel as order_channel,
    sum(se.shipping_cost) as shipping_revenue,
    sum(se.actual_shipping_cost) as shipping_cost,
    sum(se.shipping_margin) as shipping_margin
  from {{ source('silver', 'int_shipping_economics') }} se
  group by 1, 2
)
select
  coalesce(finance.order_date, shipping.order_date) as order_date,
  coalesce(finance.order_channel, shipping.order_channel) as order_channel,
  finance.gross_revenue,
  finance.net_revenue,
  shipping.shipping_revenue,
  shipping.shipping_cost,
  shipping.shipping_margin
from finance
full outer join shipping
  on finance.order_date = shipping.order_date
  and finance.order_channel = shipping.order_channel
