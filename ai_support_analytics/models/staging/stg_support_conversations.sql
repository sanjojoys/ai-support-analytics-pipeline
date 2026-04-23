select
    conversation_id,
    account_id,
    user_id,
    created_at::timestamp as created_at_ts,
    resolved_at::timestamp as resolved_at_ts,
    channel,
    issue_type,
    resolved_by,
    ai_assist_used_flag::boolean as is_ai_assisted,
    escalated_flag::boolean as is_escalated,
    resolution_minutes::int as resolution_minutes,
    csat_score::float as csat_score
from {{ source('raw', 'raw_support_conversations') }}