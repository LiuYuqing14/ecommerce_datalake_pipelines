select
  order_id
from {{ ref('stg_ecommerce__orders') }}
where net_total > gross_total
