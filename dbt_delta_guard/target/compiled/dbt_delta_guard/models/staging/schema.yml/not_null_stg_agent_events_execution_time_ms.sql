
    
    



select execution_time_ms
from "dbt_delta_guard"."main"."stg_agent_events"
where execution_time_ms is null


