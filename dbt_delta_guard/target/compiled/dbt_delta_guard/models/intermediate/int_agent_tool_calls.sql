

with staging as (
    select * from "dbt_delta_guard"."main"."stg_agent_events"
)

select
    agent_id,
    session_id,
    action_id,
    timestamp,
    tool_name,
    execution_time_ms,
    cost_usd,
    status,
    case when status = 'SUCCESS' then 1 else 0 end as is_success,
    query,
    "limit",
    url,
    event_hour
from staging