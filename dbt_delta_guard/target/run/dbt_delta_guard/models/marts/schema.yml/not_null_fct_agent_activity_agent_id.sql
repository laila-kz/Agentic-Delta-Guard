select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select agent_id
from "dbt_delta_guard"."main"."fct_agent_activity"
where agent_id is null



      
    ) dbt_internal_test