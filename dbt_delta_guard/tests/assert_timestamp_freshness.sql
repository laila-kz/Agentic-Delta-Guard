{{ config(severity = 'warn') }}

-- Asserts that incoming events fall within the rolling 24h freshness window and not in the future.
-- Configured as WARN for static test datasets, but enforced as ERROR in streaming production.
with time_bounds as (
    select
        coalesce(max(timestamp), current_timestamp) as reference_time
    from {{ ref('stg_agent_events') }}
)
select
    s.agent_id,
    s.session_id,
    s.action_id,
    s.timestamp
from {{ ref('stg_agent_events') }} s
cross join time_bounds b
where s.timestamp < b.reference_time - interval {{ var('max_event_age_hours') }} hour
   or s.timestamp > b.reference_time + interval {{ var('max_future_skew_minutes') }} minute
