
  
  create view "dbt_delta_guard"."main"."stg_agent_events__dbt_tmp" as (
    

with source_events as (
	select
		cast(agent_id as varchar) as agent_id,
		cast(session_id as varchar) as session_id,
		cast(action_id as varchar) as action_id,
		cast(timestamp as timestamp) as timestamp,
		cast(tool_name as varchar) as tool_name,
		cast(execution_time_ms as integer) as execution_time_ms,
		cast(cost_usd as double) as cost_usd,
		tool_args,
		row_number() over (
			partition by agent_id, session_id, action_id
			order by timestamp desc
		) as row_num
	from read_parquet('../data/bronze/agent_events/*.parquet')
),

deduplicated_events as (
	select
		agent_id,
		session_id,
		action_id,
		timestamp,
		tool_name,
		execution_time_ms,
		cost_usd,
		json_extract_string(tool_args, '$.query') as query,
		cast(json_extract_string(tool_args, '$.limit') as integer) as "limit",
		json_extract_string(tool_args, '$.url') as url
	from source_events
	where row_num = 1
)

select
	agent_id,
	session_id,
	action_id,
	timestamp,
	tool_name,
	execution_time_ms,
	cost_usd,
	query,
	"limit",
	url,
	date_trunc('hour', timestamp) as event_hour
from deduplicated_events
  );
