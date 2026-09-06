
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    tool_name as unique_field,
    count(*) as n_records

from "dbt_delta_guard"."main"."dim_tool_efficiency"
where tool_name is not null
group by tool_name
having count(*) > 1



  
  
      
    ) dbt_internal_test