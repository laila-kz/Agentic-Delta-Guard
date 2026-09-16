select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_invocations
from "dbt_delta_guard"."main"."dim_tool_efficiency"
where total_invocations is null



      
    ) dbt_internal_test