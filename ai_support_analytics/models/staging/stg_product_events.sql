
select
    event_id,
    account_id,
    user_id,
    session_id,
    event_ts::timestamp as event_ts,
    event_name,
    feature_name,
    page_name,
    parse_json(properties_json) as event_properties, -- Parsing JSON
    ingested_at::timestamp as ingested_at_ts
from {{ source('raw', 'raw_product_events') }}
