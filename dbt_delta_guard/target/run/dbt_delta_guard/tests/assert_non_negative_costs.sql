
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
    agent_id,
    session_id,
    action_id,
    cost_usd,
    execution_time_ms
from "dbt_delta_guard"."main"."stg_agent_events"
where cost_usd < 0.0
   or execution_time_ms < 0
   or cost_usd > 50.0
  
  
      
    ) dbt_internal_test