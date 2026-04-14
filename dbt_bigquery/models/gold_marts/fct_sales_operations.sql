select
  sv.product_id,
  date(sv.order_dt) as order_date,
  avg(sv.velocity_avg) as sales_velocity_7d,
  max(sv.trend_signal) as trend_signal,
  max(ir.inventory_quantity) as inventory_quantity,
  max(ir.risk_tier) as inventory_risk_tier,
  max(pp.gross_profit) as gross_profit,
  max(pp.net_margin) as net_margin,
  max(pp.return_rate) as return_rate
from {{ source('silver', 'int_sales_velocity') }} sv
left join {{ source('silver', 'int_inventory_risk') }} ir
  on sv.product_id = ir.product_id
left join {{ source('silver', 'int_product_performance') }} pp
  on sv.product_id = pp.product_id
  and sv.order_dt = pp.product_dt
group by 1, 2
