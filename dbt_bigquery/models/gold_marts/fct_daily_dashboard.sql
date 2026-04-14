select
  date(date) as metric_date,
  orders_count,
  gross_revenue,
  net_revenue,
  avg_order_value,
  carts_created,
  cart_conversion_rate,
  returns_count,
  return_rate,
  refund_total,
  revenue_7d_avg,
  revenue_30d_avg,
  revenue_30d_std,
  revenue_anomaly_flag
from {{ source('silver', 'int_daily_business_metrics') }}
