select
  date(cart_dt) as cart_date,
  coalesce(order_channel, 'unknown') as channel,
  countif(cart_status = 'abandoned') as abandoned_carts,
  countif(cart_status = 'converted') as converted_carts,
  safe_divide(countif(cart_status = 'converted'), count(*)) as conversion_rate,
  sum(abandoned_value) as abandoned_value,
  avg(time_to_purchase_hours) as avg_time_to_purchase_hours
from {{ source('silver', 'int_cart_attribution') }}
group by 1, 2
