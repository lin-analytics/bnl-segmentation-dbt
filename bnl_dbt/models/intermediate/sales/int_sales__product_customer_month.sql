with resolved_sales as (
    select *
    from {{ref('int_customer__resolved')}}
),

final as (
    select
        date_trunc('month', invoice_date) as month,
        bu,
        product_id,
        analysis_customer_id,
        sum(net_sales) as sales_amt
    from resolved_sales
    group by 1,2,3,4
)

select * from final