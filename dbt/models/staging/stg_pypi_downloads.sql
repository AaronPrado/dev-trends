with silver as (
    select * from delta.`{{ var("pypi_silver_path") }}`
)
select
    technology,
    pypi_package,
    download_count,
    download_date,
    'pypi' as source
from silver