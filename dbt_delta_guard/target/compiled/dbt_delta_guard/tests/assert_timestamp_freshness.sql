

-- Asserts that incoming events fall within the rolling 24h freshness window and not in the future.
-- Configured as WARN for static test datasets, but enforced as ERROR in streaming production.
with time_bounds as (
    select
        coalesce(max(timestamp), current_timestamp) as reference_time
    from "dbt_delta_guard"."main"."stg_agent_events"
)
select
    s.agent_id,
    s.session_id,
    s.action_id,
    s.timestamp
from "dbt_delta_guard"."main"."stg_agent_events" s
cross join time_bounds b
where s.timestamp < b.reference_time - interval 24 hour
   or s.timestamp > b.reference_time + interval 5 minute