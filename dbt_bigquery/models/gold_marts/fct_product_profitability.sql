select
  product_id,
  date(product_dt) as product_date,
  units_sold,
  units_returned,
  gross_revenue,
  net_revenue,
  gross_profit,
  net_margin,
  return_rate,
  margin_pct
from {{ source('silver', 'int_product_performance') }}
