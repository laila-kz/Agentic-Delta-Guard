
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select success_rate_pct
from "dbt_delta_guard"."main"."dim_tool_efficiency"
where success_rate_pct is null



  
  
      
    ) dbt_internal_test