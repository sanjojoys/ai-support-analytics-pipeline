with events as (
    select * from {{ ref('int_events_deduped') }}
),

lagged_events as (
    select
        *,
        lag(event_ts) over (partition by user_id order by event_ts) as previous_event_at
    from events
),

new_session_flags as (
    select
        *,
        case 
            when previous_event_at is null then 1
            when timediff(minute, previous_event_at, event_ts) >= 30 then 1
            else 0
        end as is_new_session
    from lagged_events
)

select
    *,
    sum(is_new_session) over (partition by user_id order by event_ts rows between unbounded preceding and current row) as calculated_session_id
from new_session_flags