select
  customer_segment,
  predicted_clv_bucket,
  actual_clv_bucket,
  count(*) as customer_count,
  avg(net_clv) as avg_net_clv,
  avg(avg_order_value) as avg_order_value,
  avg(total_spent) as avg_total_spent
from {{ source('silver', 'int_customer_lifetime_value') }}
group by 1, 2, 3
