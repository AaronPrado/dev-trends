{{ config(severity='warn') }}

with daily as (
    select
        activity_date,
        technology,
        sum(case when event_type = 'push' then event_count else 0 end) as push_count,
        sum(case when event_type = 'pull_request' then event_count else 0 end) as pr_count
    from {{ ref('fact_github_activity') }}
    group by activity_date, technology
)
select
    activity_date,
    technology,
    push_count,
    pr_count
from daily
where push_count >= 100
  and pr_count <= 5
