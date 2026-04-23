select
    a.*,
    o.setup_completed_at,
    o.first_ai_interaction_at,
    r.is_retained_d28
from {{ ref('stg_accounts') }} a
left join {{ ref('int_onboarding_milestones') }} o on a.account_id = o.account_id
left join {{ ref('int_retention_flags') }} r on a.account_id = r.account_id