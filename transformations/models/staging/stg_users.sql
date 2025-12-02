{{ config(materialized='table') }}

SELECT
    gh_avatar,
	gh_id,
	gh_login,
	id,
	name
FROM {{ source('raw', 'users') }}