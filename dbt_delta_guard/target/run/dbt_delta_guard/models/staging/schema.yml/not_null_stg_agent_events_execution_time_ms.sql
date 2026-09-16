select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select execution_time_ms
from "dbt_delta_guard"."main"."stg_agent_events"
where execution_time_ms is null



      
    ) dbt_internal_test