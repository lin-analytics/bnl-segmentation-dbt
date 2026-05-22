with source_data as (
    select *
    from {{ref('sales_transactions')}}
)

select
    cast(invoice_date as date) as invoice_date,
    cast(bu as string) as bu,
    cast(product_id as string) as product_id,
    cast(customer_id as string) as customer_id,
    cast(net_sales as float64) as net_sales,
from source_data