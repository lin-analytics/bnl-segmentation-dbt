with months as (
    select distinct
        as_of_month as month
    from {{ ref('fct_customer_segmentation__product_customer_month') }}
),

final as (
    select
        month,
        extract(year from month) as year,
        extract(month from month) as month_number,
        format_date('%Y-%m', month) as year_month,
        case
            when extract(month from month) <= 6 then 'H1'
            else 'H2'
        end as half_year
    from months
)

select * from final