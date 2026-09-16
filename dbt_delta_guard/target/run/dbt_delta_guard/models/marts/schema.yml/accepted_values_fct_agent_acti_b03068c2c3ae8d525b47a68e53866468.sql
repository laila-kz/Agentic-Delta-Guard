select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        latency_tier as value_field,
        count(*) as n_records

    from "dbt_delta_guard"."main"."fct_agent_activity"
    group by latency_tier

)

select *
from all_values
where value_field not in (
    'FAST (<100ms)','NOMINAL (100-500ms)','SLOW (>500ms)'
)



      
    ) dbt_internal_test