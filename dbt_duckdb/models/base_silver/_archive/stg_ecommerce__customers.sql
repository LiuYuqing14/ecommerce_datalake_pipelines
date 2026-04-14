select *
from {{ source('bronze', 'customers') }}
