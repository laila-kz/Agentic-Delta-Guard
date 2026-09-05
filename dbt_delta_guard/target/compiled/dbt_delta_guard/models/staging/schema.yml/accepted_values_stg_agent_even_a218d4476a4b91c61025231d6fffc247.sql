
    
    

with all_values as (

    select
        tool_name as value_field,
        count(*) as n_records

    from "dbt_delta_guard"."main"."stg_agent_events"
    group by tool_name

)

select *
from all_values
where value_field not in (
    'sql_query_executor','vector_search','web_scraper','db_writer'
)


