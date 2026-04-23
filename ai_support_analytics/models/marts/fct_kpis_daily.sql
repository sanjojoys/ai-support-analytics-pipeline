select
    h.usage_date,
    h.account_id,
    h.total_ai_queries,
    h.health_score,
    h.daily_tickets,
    a.plan_tier,
    a.region
from {{ ref('int_account_health_daily') }} h
left join {{ ref('stg_accounts') }} a on h.account_id = a.account_id