{{ config(
    materialized='incremental',
    on_schema_change='fail',
    unique_key=['version_id', 'date']
) }}

SELECT
    version_id,
    downloads,
    date::date as date
FROM {{ source('raw', 'version_downloads') }}
WHERE downloads >= 0

{% if is_incremental() %}
  AND date > (SELECT MAX(date) FROM {{ this }})
  AND date < date_trunc('day', now())
{% endif %}