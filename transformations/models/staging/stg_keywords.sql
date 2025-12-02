{{ config(materialized='table') }}

SELECT
    crates_cnt,
	created_at at time zone 'UTC' as created_at,
	id,
	keyword
FROM {{ source('raw', 'keywords') }}