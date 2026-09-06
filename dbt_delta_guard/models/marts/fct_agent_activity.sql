{{ config(materialized='table') }}

select
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
	end as cost_tier,
	case when status = 'SUCCESS' then 1 else 0 end as is_success
from {{ ref('stg_agent_events') }}
