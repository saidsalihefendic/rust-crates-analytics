{{ config(materialized='table') }}

SELECT
    category_id,
    crate_id
FROM {{ source('raw', 'crates_categories') }}