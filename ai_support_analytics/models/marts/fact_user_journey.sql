select
    s.*,
    e.experiment_name,
    e.variant as experiment_variant
from {{ ref('int_sessionized_events') }} s
left join {{ ref('stg_experiment_assignments') }} e 
    on s.user_id = e.user_id 
    and s.event_ts >= e.assigned_at_ts