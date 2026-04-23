with account_base as (
    select * from {{ ref('stg_accounts') }}
),

usage as (
    select 
        account_id,
        max(usage_date) as last_active_date
    from {{ ref('int_ai_usage_daily') }}
    group by 1
)

select
    a.account_id,
    a.created_at,
    u.last_active_date,
    datediff(day, a.created_at, u.last_active_date) as days_since_signup,
    case when days_since_signup >= 28 then 1 else 0 end as is_retained_d28
from account_base a
left join usage u on a.account_id = u.account_id