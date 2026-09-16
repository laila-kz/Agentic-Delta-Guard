
    
    



select extracted_at
from "dbt_delta_guard"."main"."stg_agent_events"
where extracted_at is null


