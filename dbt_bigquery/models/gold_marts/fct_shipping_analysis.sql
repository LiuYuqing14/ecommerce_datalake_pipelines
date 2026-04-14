select
  date(order_dt) as order_date,
  order_channel,
  shipping_speed,
  count(*) as orders,
  sum(shipping_cost) as shipping_revenue,
  sum(actual_shipping_cost) as shipping_cost,
  sum(shipping_margin) as shipping_margin,
  avg(shipping_margin_pct) as shipping_margin_pct
from {{ source('silver', 'int_shipping_economics') }}
group by 1, 2, 3
