with spine as (
    select
        month,
        bu,
        product_id,
        analysis_customer_id,
        sales_amt
    from {{ ref('int_sales__product_customer_month') }}
),

as_of_months as (
    select distinct
        month as as_of_month
    from spine
),

joined as (
    select
        a.as_of_month,
        s.month,
        s.bu,
        s.product_id,
        s.analysis_customer_id,
        s.sales_amt
    from as_of_months a
    left join spine s
        on s.month <= a.as_of_month
),

final as (
    select
        as_of_month,
        bu,
        product_id,
        analysis_customer_id,

        sum(
            case
                when month >= date_sub(as_of_month, interval 30 day)
                    and month < as_of_month
                then sales_amt
                else 0
            end
        ) as sales_recent_30d,

        sum(
            case
                when month >= date_sub(as_of_month, interval 180 day)
                    and month < date_sub(as_of_month, interval 30 day)
                then sales_amt
                else 0
            end
        ) as sales_prior_180d,

        sum(
            case
                when extract(year from month) = extract(year from as_of_month)
                    and month < as_of_month
                then sales_amt
                else 0
            end
        ) as sales_ytd_current_year,

        sum(
            case
                when extract(year from month) = extract(year from as_of_month) - 1
                then sales_amt
                else 0
            end
        ) as sales_prior_year_full_year,

        sum(
            case
                when extract(year from month) = extract(year from as_of_month) - 1
                    and month < date_sub(as_of_month, interval 1 year)
                then sales_amt
                else 0
            end
        ) as sales_ytd_prior_year,

        sum(
            case
                when month >= date_sub(as_of_month, interval 90 day)
                    and month < as_of_month
                then sales_amt
                else 0
            end
        ) as sales_recent_90d,

        sum(
            case
                when month >= date_sub(as_of_month, interval 360 day)
                    and month < date_sub(as_of_month, interval 90 day)
                then sales_amt
                else 0
            end
        ) as sales_prior_360d

    from joined
    group by 1, 2, 3, 4
)

select
    *,
    sales_ytd_current_year - sales_ytd_prior_year as yoy_variance_amt
from final