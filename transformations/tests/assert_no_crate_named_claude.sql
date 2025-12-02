-- tests/assert_no_crate_named_claude.sql
-- Custom test: Fails if any crate is named 'claude'
-- Returns failing rows (rows that shouldn't exist)

SELECT 
    id,
    name,
    created_at,
    'Found a crate named Claude!' AS failure_reason
FROM {{ ref('stg_crates') }}
WHERE LOWER(name) = 'claude-bot'
