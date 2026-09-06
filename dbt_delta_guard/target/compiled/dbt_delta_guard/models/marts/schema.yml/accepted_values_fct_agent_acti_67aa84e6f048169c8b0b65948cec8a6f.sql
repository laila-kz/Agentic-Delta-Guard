
    
    

with all_values as (

    select
        cost_tier as value_field,
        count(*) as n_records

    from "dbt_delta_guard"."main"."fct_agent_activity"
    group by cost_tier

)

select *
from all_values
where value_field not in (
    'FREE','LOW_COST (<$0.01)','MEDIUM_COST ($0.01-$0.10)','HIGH_COST (>=$0.10)'
)


