select
    cast(as_of_month as date) as as_of_month,
    cast(bu as string) as bu,
    cast(analysis_customer_id as string) as analysis_customer_id,
    cast(product_id as string) as product_id,
    cast(override_bucket as string) as override_bucket,
    cast(reason as string) as reason
from {{ ref('customer_segmentation_bucket_overrides') }}