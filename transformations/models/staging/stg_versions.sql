{{ config(materialized='table') }}

SELECT
    bin_names,
	categories,
	checksum,
	crate_id,
	crate_size,
	created_at at time zone 'UTC' as created_at,
	description,
	documentation,
	downloads,
	edition,
	features,
	has_lib,
	homepage,
	id,
	keywords,
	license,
	links,
	num,
	num_no_build,
	published_by,
	repository,
	rust_version,
	updated_at at time zone 'UTC' as updated_at,
	yanked
FROM {{ source('raw', 'versions') }}