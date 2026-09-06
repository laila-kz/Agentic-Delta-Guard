select
    action_id,
    count(*) as duplicate_count
from {{ ref('fct_agent_activity') }}
group by action_id
having count(*) > 1
