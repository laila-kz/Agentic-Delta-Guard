
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
    agent_id,
    session_id,
    action_id,
    timestamp,
    cost_usd
from "dbt_delta_guard"."main"."stg_agent_events"
where cost_usd > 1.00
  
  
      
    ) dbt_internal_test