select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    action_id as unique_field,
    count(*) as n_records

from "dbt_delta_guard"."main"."fct_agent_activity"
where action_id is not null
group by action_id
having count(*) > 1



      
    ) dbt_internal_test