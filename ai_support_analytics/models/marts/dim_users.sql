select
    u.user_id,
    u.account_id,
    u.signup_at_ts,
    u.role_type,
    u.country,
    u.acquisition_channel,
    a.account_name,
    a.plan_tier
from {{ ref('stg_users') }} u
left join {{ ref('stg_accounts') }} a on u.account_id = a.account_id