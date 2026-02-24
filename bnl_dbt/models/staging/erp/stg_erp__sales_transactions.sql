with source_data as (
    select *
    from {{ref('sales_transactions')}}
)

select
    cast(invoice_date as date) as invoice_date,
    cast(bu as varchar) as bu,
    cast(product_id as varchar) as product_id,
    cast(customer_id as varchar) as customer_id,
    cast(net_sales as double) as net_sales,
from source_data