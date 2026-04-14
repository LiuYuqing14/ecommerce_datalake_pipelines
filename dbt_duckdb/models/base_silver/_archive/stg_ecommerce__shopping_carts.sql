select *
from {{ source('bronze', 'shopping_carts') }}
