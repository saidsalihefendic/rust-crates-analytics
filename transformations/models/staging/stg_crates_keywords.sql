{{ config(materialized='table') }}

SELECT
    crate_id,
    keyword_id
FROM {{ source('raw', 'crates_keywords') }}