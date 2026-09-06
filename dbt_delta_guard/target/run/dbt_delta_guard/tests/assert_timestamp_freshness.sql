
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
    agent_id,
    session_id,
    action_id,
    timestamp
from "dbt_delta_guard"."main"."stg_agent_events"
where timestamp < current_timestamp - interval 24 hour
   or timestamp > current_timestamp + interval 5 minute
  
  
      
    ) dbt_internal_test