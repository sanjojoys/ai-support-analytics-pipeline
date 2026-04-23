select
    user_id,
    account_id,
    signup_at::timestamp as signup_at_ts,
    role_type,
    country,
    acquisition_channel,
    workspace_connected_at::timestamp as workspace_connected_at_ts,
    invited_teammates_count::int as invited_teammates_count
from {{ source('raw', 'raw_users') }}
