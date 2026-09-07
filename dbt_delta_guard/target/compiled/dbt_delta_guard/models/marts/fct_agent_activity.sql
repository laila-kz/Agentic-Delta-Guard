

with __dbt__cte__int_agent_tool_calls as (


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
) select
	*,
	case
		when execution_time_ms < 100 then 'FAST (<100ms)'
		when execution_time_ms between 100 and 500 then 'NOMINAL (100-500ms)'
		else 'SLOW (>500ms)'
	end as latency_tier,
	case
		when cost_usd = 0.0 then 'FREE'
		when cost_usd < 0.01 then 'LOW_COST (<$0.01)'
		when cost_usd < 0.1 then 'MEDIUM_COST ($0.01-$0.10)'
		else 'HIGH_COST (>=$0.10)'
	end as cost_tier
from __dbt__cte__int_agent_tool_calls