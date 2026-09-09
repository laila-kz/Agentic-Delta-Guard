select
    agent_id,
    session_id,
    action_id,
    timestamp
from {{ ref('stg_agent_events') }}
where timestamp < current_timestamp - interval {{ var('max_event_age_hours') }} hour
    or timestamp > current_timestamp + interval {{ var('max_future_skew_minutes') }} minute
