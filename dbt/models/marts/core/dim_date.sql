-- Spine de fechas acotada al rango real de los datos, UNIÓN de todas las fuentes
-- (GitHub + PyPI): dim_date cubre el rango de cualquier hecho.
-- Cada fuente se reduce a una fila (min/max) antes de unir, no se escanea entera.
with bounds as (
    select min(activity_date) as min_date, max(activity_date) as max_date
    from {{ ref('stg_github_events') }}
    union all
    select min(download_date) as min_date, max(download_date) as max_date
    from {{ ref('stg_pypi_downloads') }}
),
overall as (
    select min(min_date) as min_date, max(max_date) as max_date
    from bounds
),
spine as (
    select explode(sequence(min_date, max_date, interval 1 day)) as date_day
    from overall
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