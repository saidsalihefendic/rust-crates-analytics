{{ config(materialized='table') }}

SELECT
    crate_id,
	downloads
FROM {{ source('raw', 'crate_downloads') }}