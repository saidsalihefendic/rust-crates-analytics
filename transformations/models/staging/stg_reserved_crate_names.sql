{{ config(materialized='table') }}

SELECT
    name
FROM {{ source('raw', 'reserved_crate_names') }}