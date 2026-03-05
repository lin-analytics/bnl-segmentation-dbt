select
    cast(product_id as varchar) as product_id,
    cast(product_name as varchar) as product_name,
    cast(bu as varchar) as bu
from {{ ref('product_master') }}