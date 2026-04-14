select *
from {{ source('bronze', 'product_catalog') }}
