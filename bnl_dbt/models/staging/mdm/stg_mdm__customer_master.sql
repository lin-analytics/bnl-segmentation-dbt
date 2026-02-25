with source_data as (
    select * from {{ref('customer_master')}}
)

select 
    cast(customer_id as varchar) as customer_id,
    cast(customer_name as varchar) as customer_name,
from source_data