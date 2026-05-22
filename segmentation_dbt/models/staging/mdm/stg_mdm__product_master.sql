select
    cast(product_id as string) as product_id,
    cast(product_name as string) as product_name,
    cast(bu as string) as bu
from {{ ref('product_master') }}