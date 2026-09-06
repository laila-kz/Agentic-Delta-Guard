select
    agent_id,
    session_id,
    action_id,
    timestamp,
    cost_usd
from {{ ref('stg_agent_events') }}
where cost_usd > 1.00
