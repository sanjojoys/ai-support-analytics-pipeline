with usage as (
    select * from {{ ref('int_ai_usage_daily') }}
),

support as (
    select 
        account_id,
        date_trunc('day', created_at_ts) as support_date,
        count(*) as ticket_count
    from {{ ref('int_support_enriched') }}
    group by 1, 2
)

select
    u.account_id,
    u.usage_date,
    u.total_ai_queries,
    coalesce(s.ticket_count, 0) as daily_tickets,
    -- Simple logic: High AI usage + Low tickets = Healthy
    (u.total_ai_queries * 10) - (coalesce(s.ticket_count, 0) * 5) as health_score
from usage u
left join support s 
    on u.account_id = s.account_id 
    and u.usage_date = s.support_date