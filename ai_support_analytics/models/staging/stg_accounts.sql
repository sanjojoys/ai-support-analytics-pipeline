select
    account_id,
    account_name,
    created_at::timestamp as created_at,
    plan_tier,
    industry,
    region,
    company_size_bucket,
    billing_status
from {{ source('raw', 'raw_accounts') }}