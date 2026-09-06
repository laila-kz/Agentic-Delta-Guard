
    
    

select
    tool_name as unique_field,
    count(*) as n_records

from "dbt_delta_guard"."main"."dim_tool_efficiency"
where tool_name is not null
group by tool_name
having count(*) > 1


