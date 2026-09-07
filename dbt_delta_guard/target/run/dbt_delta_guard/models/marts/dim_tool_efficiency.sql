
  
    
    

    create  table
      "dbt_delta_guard"."main"."dim_tool_efficiency__dbt_tmp"
  
    as (
      

select
	tool_name,
	count(*) as total_invocations,
	count(distinct agent_id) as unique_agents_using_tool,
	round(sum(cost_usd), 4) as total_cost_usd,
	round(avg(cost_usd), 6) as avg_cost_per_invocation_usd,
	round(avg(execution_time_ms), 6) as avg_latency_ms,
	approx_quantile(execution_time_ms, 0.95) as p95_latency_ms,
	sum(is_success) as successful_executions,
	sum(case when is_success = 0 then 1 else 0 end) as failed_executions,
	round(100.0 * sum(is_success) / count(*), 6) as success_rate_pct,
	round(100.0 * sum(case when cost_usd = 0.0 then 1 else 0 end) / count(*), 6) as free_invocation_pct,
	round(avg(case when is_success = 1 then execution_time_ms end), 6) as avg_success_latency_ms,
	round(avg(case when is_success = 0 then execution_time_ms end), 6) as avg_failure_latency_ms
from "dbt_delta_guard"."main"."fct_agent_activity"
group by tool_name
    );
  
  