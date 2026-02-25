with source_data as(
    select * from {{ref('customer_merge_map')}}
)

select
    cast(old_customer_id as varchar) as old_customer_id,
    cast(survivor_customer_id as varchar) as survivor_customer_id,
    cast(reason as varchar) as reason
from source_data