-- Descargas de PyPI a grano diario (fecha × tecnología × fuente). Mide ADOPCIÓN.
-- La suma paquete→tecnología se hace aquí; Silver conserva el grano por paquete.
with downloads as (
    select * from {{ ref('stg_pypi_downloads') }}
),
aggregated as (
    select
        download_date,
        technology,
        source,
        sum(download_count) as download_count
    from downloads
    group by download_date, technology, source
)
select
    md5(concat_ws('|', cast(download_date as string), technology, source)) as download_key,
    download_date,
    technology,
    source,
    download_count
from aggregated
