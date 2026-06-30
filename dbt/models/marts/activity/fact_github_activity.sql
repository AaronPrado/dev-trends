-- Actividad de desarrollo a grano diario (fecha × tecnología × tipo × fuente).
-- La evolución mensual es un rollup vía dim_date, no un pre-agregado al mes.
with events as (
    select * from {{ ref('stg_github_events') }}
),
aggregated as (
    select
        activity_date,
        technology,
        event_type,
        source,
        count(*) as event_count
    from events
    group by activity_date, technology, event_type, source
)
select
    md5(concat_ws('|', cast(activity_date as string), technology, event_type, source)) as activity_key,
    activity_date,
    technology,
    event_type,
    source,
    event_count
from aggregated