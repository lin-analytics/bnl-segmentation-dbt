with months as (
    select distinct
        as_of_month as month
    from {{ ref('fct_bnl__product_customer_month') }}
),

final as (
    select
        month,
        year(month) as year,
        month(month) as month_number,
        strftime(month, '%Y-%m') as year_month,
        case when month(month) <= 6 then 'H1' else 'H2' end as half_year
    from months
)

select * from final