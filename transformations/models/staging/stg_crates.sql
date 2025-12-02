{{ config(materialized='table') }}

SELECT
    created_at at time zone 'UTC' as created_at,
    description,
    documentation,
    homepage,
    id,
    max_features,
    max_upload_size,
    name,
    readme,
    repository,
    updated_at at time zone 'UTC' as updated_at
FROM {{ source('raw', 'crates') }}