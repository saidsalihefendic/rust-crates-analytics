{{ config(materialized='table') }}

SELECT
    category,
	crates_cnt,
	created_at at time zone 'UTC' as created_at,
	description,
	id,
	path,
	slug
FROM {{ source('raw', 'categories') }}