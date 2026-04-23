with events as (
    select * from {{ ref('int_events_deduped') }}
),

milestones as (
    select
        account_id,
        min(case when event_name = 'account_created' then event_ts end) as created_at,
        min(case when event_name = 'setup_complete' then event_ts end) as setup_completed_at,
        min(case when event_name = 'first_ai_query' then event_ts end) as first_ai_interaction_at
    from events
    group by 1
)

select * from milestones