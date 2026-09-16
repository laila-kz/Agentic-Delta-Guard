select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select action_id
from "dbt_delta_guard"."main"."fct_agent_activity"
where action_id is null



      
    ) dbt_internal_test