select
    experiment_id,
    experiment_name,
    user_id,
    account_id,
    assigned_at::timestamp as assigned_at_ts,
    variant
from {{ source('raw', 'raw_experiment_assignments') }}
