with support as (
    select * from {{ ref('stg_support_conversations') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
)

select
    s.*,
    a.plan_tier,
    case 
        when s.is_escalated = true then 1 
        else 0 
    end as is_high_priority_escalation
from support s
left join accounts a on s.account_id = a.account_id