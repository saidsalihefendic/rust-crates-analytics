{{ config(materialized='table') }}

SELECT
    crate_id,
    num_versions,
    version_id
FROM {{ source('raw', 'default_versions') }}