select *
from {{ source('bronze', 'order_items') }}
