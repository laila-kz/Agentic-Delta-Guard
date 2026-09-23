{{ config(materialized='view') }}

-- Reads Bronze Delta Lake table via DuckDB's delta extension (delta_scan reads
-- the _delta_log transaction log so it finds all Parquet files regardless of how
-- deeply nested they are under the table root).
-- Fallback: read_parquet with ** recursive glob for non-Delta Parquet exports.

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
	-- delta_scan() reads _delta_log → finds all part-*.parquet files, no glob needed
	from delta_scan('../data/bronze/agent_events')
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
