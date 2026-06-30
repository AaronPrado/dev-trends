-- Spine de fechas acotada al rango real de los datos (min/max de la actividad):
-- evita generar fechas fuera del periodo cargado.
with bounds as (
    select min(activity_date) as min_date, max(activity_date) as max_date
    from {{ ref('stg_github_events') }}
),
spine as (
    select explode(sequence(min_date, max_date, interval 1 day)) as date_day
    from bounds
)
select
    date_day,
    year(date_day)                as year,
    month(date_day)               as month,
    day(date_day)                 as day,
    weekofyear(date_day)          as iso_week,
    date_format(date_day, 'EEEE') as weekday,
    dayofweek(date_day) in (1, 7) as is_weekend
from spine