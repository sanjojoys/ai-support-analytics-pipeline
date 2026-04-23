with events as (
    select * from {{ ref('stg_product_events') }}
)

select *
from events
qualify row_number() over (
    partition by event_id 
    order by event_ts desc
) = 1