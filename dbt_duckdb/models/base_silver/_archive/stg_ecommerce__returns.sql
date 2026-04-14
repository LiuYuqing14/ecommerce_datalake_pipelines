select *
from {{ source('bronze', 'returns') }}
