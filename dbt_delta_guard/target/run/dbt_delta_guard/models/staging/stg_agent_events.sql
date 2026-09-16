
  
  create view "dbt_delta_guard"."main"."stg_agent_events__dbt_tmp" as (
    

-- NOTE: This reads from Bronze layer parquet exports
-- In production (dbt-spark), we would read directly from delta.`../data/bronze/agent_events`
-- For now, both duckdb (local dev) and spark (prod) can read the parquet format

with raw_bronze as (
	select
		agent_id,
		session_id,
		action_id,
		cast(timestamp as timestamp) as event_timestamp,
		tool_name,
		cast(execution_time_ms as integer) as execution_time_ms,
		cast(cost_usd as double) as cost_usd,
		status,
		tool_args
	from read_parquet('../data/bronze/agent_events/*.parquet')
),

deduplicated as (
	select
		*,
		row_number() over (
			partition by agent_id, session_id, action_id
			order by event_timestamp desc
		) as row_num
	from raw_bronze
)

select
	agent_id,
	session_id,
	action_id,
	event_timestamp as timestamp,
	tool_name,
	execution_time_ms,
	cost_usd,
	status,
	tool_args,
	json_extract_string(tool_args, '$.query') as query,
	cast(json_extract_string(tool_args, '$.limit') as integer) as "limit",
	json_extract_string(tool_args, '$.url') as url,
	date_trunc('hour', event_timestamp) as event_hour,
	current_timestamp as extracted_at
from deduplicated
where row_num = 1
  );
