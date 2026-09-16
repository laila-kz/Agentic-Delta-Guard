select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      select
    action_id,
    count(*) as duplicate_count
from "dbt_delta_guard"."main"."fct_agent_activity"
group by action_id
having count(*) > 1
      
    ) dbt_internal_test