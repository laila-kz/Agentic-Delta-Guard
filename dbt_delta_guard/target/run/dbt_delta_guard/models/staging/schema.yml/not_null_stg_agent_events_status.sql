select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select status
from "dbt_delta_guard"."main"."stg_agent_events"
where status is null



      
    ) dbt_internal_test