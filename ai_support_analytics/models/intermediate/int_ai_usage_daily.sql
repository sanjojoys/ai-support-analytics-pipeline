with events as (
    select * from {{ ref('int_events_deduped') }}
),

daily_usage as (
    select
        account_id,
        date_trunc('day', event_ts) as usage_date,
        count_if(event_name = 'ai_query') as total_ai_queries,
        count_if(event_name = 'ai_response_accepted') as total_ai_accepts
    from events
    group by 1, 2
)

select * from daily_usage