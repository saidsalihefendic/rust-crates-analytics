{{ config(materialized='table') }}

SELECT
    crate_id,
    created_at at time zone 'UTC' as created_at,
    created_by,
    owner_id,
    owner_kind
FROM {{ source('raw', 'crate_owners') }}