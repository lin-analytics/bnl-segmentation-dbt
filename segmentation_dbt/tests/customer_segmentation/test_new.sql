select *
from {{ ref('int_customer_segmentation__bucket_final') }} bf
left join {{ ref('int_customer_segmentation__sales_windows') }} w
  on bf.as_of_month = w.as_of_month
 and bf.bu = w.bu
 and bf.product_id = w.product_id
 and bf.analysis_customer_id = w.analysis_customer_id
where bf.logic_type = 'standard'
  and bf.customer_segmentation_bucket_final = 'New'
  and not (
      w.sales_recent_30d > 0
      and w.sales_prior_180d = 0
  )