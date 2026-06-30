with silver as (
    select * from delta.`{{ var("silver_path") }}`
)
select
    event_id,
    technology,
    event_type,
    repository,
    organization,
    created_at,
    cast(created_at as date) as activity_date,
    'github' as source
from silver