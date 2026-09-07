-- Fails if any recorded agent action has invalid financial or runtime metrics
select
    action_id,
    agent_id,
    cost_usd,
    execution_time_ms
from "dbt_delta_guard"."main"."stg_agent_events"
where cost_usd < 0.0 
   or execution_time_ms < 0 
   or cost_usd > 50.0