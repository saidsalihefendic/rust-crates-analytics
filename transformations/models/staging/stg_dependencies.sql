{{ config(materialized='table') }}

SELECT
    crate_id,
	default_features,
	explicit_name,
	features,
	id,
	kind,
	optional,
	req,
	target,
	version_id
FROM {{ source('raw', 'dependencies') }}