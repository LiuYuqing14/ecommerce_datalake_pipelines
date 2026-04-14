select *
from {{ source('bronze', 'return_items') }}
