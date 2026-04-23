with support as (
    select * from {{ ref('int_support_enriched') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
)

select
    -- Columns from Support
    s.conversation_id,
    s.account_id,
    s.user_id,
    s.created_at_ts,
    s.resolved_at_ts,
    s.channel,
    s.issue_type,
    s.is_ai_assisted,
    s.is_escalated,
    s.is_high_priority_escalation,
    s.resolution_minutes,
    s.csat_score,
    
    -- Columns from Accounts (the unique ones)
    a.account_name,
    a.plan_tier as account_tier,
    a.industry,
    a.region
from support s
left join accounts a on s.account_id = a.account_id