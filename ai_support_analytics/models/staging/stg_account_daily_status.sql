select
    snapshot_date::date as snapshot_date,
    account_id,
    active_users_7d::int as active_users_7d,
    conversations_7d::int as conversations_7d,
    ai_suggestions_accepted_7d::int as ai_suggestions_accepted_7d,
    tickets_7d::int as tickets_7d,
    escalations_7d::int as escalations_7d,
    renewal_due_in_30d_flag::boolean as is_renewal_due_soon,
    churned_flag::boolean as is_churned
from {{ source('raw', 'raw_account_daily_status') }}
