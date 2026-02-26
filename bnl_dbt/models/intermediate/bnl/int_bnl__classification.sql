with w as (
    select *
    from {{ref('int_bnl__sales_windows')}}
),

bucketed as (
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
        yoy_variance_amt,
        
        case
            when month(as_of_month) <=6 then
                case
                    when sales_recent_6_months = 0 and sales_prior_6_18_months > 0 then 'Lost'
                    when sales_recent_6_months > 0 and sales_prior_6_18_months = 0 then 'New'
                    else 'Base'
                end
            else
                case
                    when sales_ytd_current_year = 0 and sales_ytd_prior_year > 0 then 'Lost'
                    when sales_ytd_current_year > 0 and sales_ytd_prior_year = 0 then 'New'
                    else 'Base'
                end
        end as bnl_bucket
    from w
),

final as (
    select 
        *,
        case
            when bnl_bucket = 'Base' and yoy_variance_amt > 0 then 'Base Gainer'
            when bnl_bucket = 'Base' and yoy_variance_amt < 0 then 'Base Drainer'
            else bnl_bucket
        end as bnl_classification
    from bucketed 
)

select * from final