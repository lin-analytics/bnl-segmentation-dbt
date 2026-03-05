with resolved as (
    select *
    from {{ref('int_customer__resolved')}}
),

customer_master as (
    select *
    from {{ref('stg_mdm__customer_master')}}
),

final as (
    select 
        r.analysis_customer_id,
        r.customer_id as raw_customer_id,
        r.merge_applied_flg,
        cm.customer_name
    from resolved r
    left join customer_master cm
    on r.analysis_customer_id = cm.customer_id
)

select * from final