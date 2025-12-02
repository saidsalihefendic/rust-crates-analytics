{{ config(materialized='table') }}

SELECT
    avatar,
	github_id,
	id,
	login,
	name,
	org_id
FROM {{ source('raw', 'teams') }}