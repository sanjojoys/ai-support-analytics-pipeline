-- Tickets should not be resolved before they are created.
-- Returns rows where logic is broken.
select
    conversation_id,
    created_at_ts,
    resolved_at_ts
from {{ ref('fct_support_interactions') }}
where resolved_at_ts < created_at_ts