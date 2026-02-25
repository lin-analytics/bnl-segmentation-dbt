with sales as (
    select * from {{ref('stg_erp__sales_transactions')}}
),

merge_map as (
    select * from {{ref('stg_revops__customer_merge_map')}}
),

resolved as (
    select 
        sales.*,
        coalesce(mm.survivor_customer_id, sales.customer_id) as analysis_customer_id,
        case when mm.survivor_customer_id is not null then 1 else 0 end as merge_applied_flg
    from sales
    left join merge_map mm
        on sales.customer_id = mm.old_customer_id
)

select * from resolved