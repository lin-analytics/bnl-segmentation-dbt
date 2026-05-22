with resolved as (
    select *
    from {{ ref('int_customer__resolved') }}
),

customer_master as (
    select *
    from {{ ref('stg_mdm__customer_master') }}
),

final as (
    select
        r.analysis_customer_id,

        string_agg(distinct r.customer_id, ', ' order by r.customer_id) as raw_customer_ids,

        max(r.merge_applied_flg) as merge_applied_flg,

        max(cm.customer_name) as customer_name

    from resolved r
    left join customer_master cm
        on r.analysis_customer_id = cm.customer_id

    group by
        r.analysis_customer_id
)

select * from final