with spine as (
    select
        month,
        bu,
        product_id,
        analysis_customer_id,
        sales_amt
    from {{ref('int_sales__product_customer_month')}}
),

as_of_months as (
    -- build an as-of frame from the months that exist in the data
    select distinct
        month as as_of_month
    from spine
),

joined as (
    -- for each as_of_month, bring in history rows up to that month
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
        sales_recent_6_months,
        sales_prior_6_18_months,
        sales_ytd_current_year,
        sales_prior_year_full_year,
        sales_ytd_prior_year,
        (sales_ytd_current_year - sales_ytd_prior_year) as yoy_variance_amt
    from(
        select
            as_of_month,
            bu,
            product_id,
            analysis_customer_id,

    -- Recent 6 months including as_of_month(not inlcuding as_of_month)
    sum(
        case
            when month >= (as_of_month - interval '6 month') 
            and month < as_of_month then sales_amt
            else 0
        end
    ) as sales_recent_6_months,

    -- Prior window: prior 6-18 months.
    sum(
        case
            when month >= (as_of_month - interval '18 month') 
            and month < (as_of_month - interval '6 month') then sales_amt
            else 0
        end
    ) as sales_prior_6_18_months,

    -- YTD Current year sales (from Jan 1 of as_of_month's year to as_of_month, not including as_of_month the as_of_month)
    sum(
        case
            when year(month) = year(as_of_month)
            and month < as_of_month then sales_amt
            else 0
        end
    ) as sales_ytd_current_year,

    -- Prior year full year sales (from Jan 1 to Dec 31 of the year prior to as_of_month's year)
    sum(
        case
            when year(month) = (year(as_of_month) - 1)
            then sales_amt
            else 0
        end
    ) as sales_prior_year_full_year,

    -- Prior year to date sales
    sum(
        case
            when year(month) = year(as_of_month) -1
            and month < (as_of_month - interval '1 year') then sales_amt
            else 0
        end
    ) as sales_ytd_prior_year
    from joined
    group by 1,2,3,4
) x
)

select * from final