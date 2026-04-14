select *
from {{ source('bronze', 'cart_items') }}
