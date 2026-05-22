with source_data as(
    select * from {{ref('customer_merge_map')}}
)

select
    cast(old_customer_id as string) as old_customer_id,
    cast(survivor_customer_id as string) as survivor_customer_id,
    cast(reason as string) as reason
from source_data