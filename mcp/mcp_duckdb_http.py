import logging
import os
import duckdb
from typing import Any
from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent, GetPromptResult, PromptMessage


logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output.log'),
    format='%(pathname)s - %(message)s'
)

def get_crates_duckdb_path():
    project_root_directory = os.path.dirname(os.path.dirname(__file__))
    data_folder = "data"
    duckdb_instance = "crates.duckdb"

    full_duckdb_path_instance = os.path.join(project_root_directory, data_folder, duckdb_instance)

    return full_duckdb_path_instance

mcp = FastMCP("duckdb_crates_server")

@mcp.tool()
async def list_tables():
    """List all tables in the staging schema"""
    crates_duckdb_path = get_crates_duckdb_path()
    conn = duckdb.connect(crates_duckdb_path, read_only=True)

    result = conn.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables t
        LEFT JOIN (
            SELECT table_schema, table_name
            FROM information_schema.columns
            GROUP BY table_schema, table_name
        ) cols USING (table_schema, table_name)
        WHERE table_schema IN ('raw', 'staging', 'marts')
        GROUP BY table_schema, table_name
        ORDER BY table_schema, table_name
    """).fetchdf()
    
    return [TextContent(
        type="text",
        text=f"Available tables:\n\n{result.to_markdown(index=False)}"
    )]

@mcp.tool()
async def read_query(sql: str):
    """Execute a read-only SQL query against the crates.io DuckDB database"""
    # Safety check
    if not sql.strip().upper().startswith('SELECT'):
        return [TextContent(
            type="text",
            text="Error: Only SELECT queries are allowed"
        )]
    
    crates_duckdb_path = get_crates_duckdb_path()
    conn = duckdb.connect(crates_duckdb_path, read_only=True)

    result = conn.execute(sql).fetchdf()
    
    return [TextContent(
        type="text",
        text=f"{result.to_markdown(index=False)}"
    )]


@mcp.resource("schema://staging/tables")
async def get_available_dbt_models():
    """Column definitions and relationships for all staging tables"""
    
    project_root = os.path.dirname(os.path.dirname(__file__))
    schema_path = os.path.join(project_root, "transformations", "models", "staging")
    
    # Read all .yml files in staging folder
    schema_content = []
    for yml_file in os.listdir(schema_path):
        if yml_file.endswith('.yml'):
            with open(os.path.join(schema_path, yml_file), 'r') as f:
                schema_content.append(f"# {yml_file}\n{f.read()}\n")
    
    return "\n".join(schema_content)

@mcp.prompt()
async def setup_crates_analytics_context():
    """Load context about the Rust Crates Analytics project"""
    return GetPromptResult(
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text="""You are exploring the Rust Crates Analytics data warehouse.

**Available Data:**
- Staging schema contains cleaned, validated data from crates.io
- Date range: figure out from the read_query tool, please
- You can expect around ~200k crates, ~1.75M versions, billions of download records

**Key Tables:**
- stg_crates: Rust packages
- stg_versions: Specific releases
- stg_version_downloads: Download history (fact table)
- stg_dependencies: Version dependencies
- stg_categories, stg_keywords: Metadata

**Available Tools:**
- query_duckdb: Run SELECT queries
- list_tables: See available tables

**Available Resources:**
- schema://staging/tables: Full schema definitions

**Setup:**
Please load the context using the Resources for schema to let you know of columns, types, data quality tests, relationships.
That way, you already know about the Rust Crates DWH structure and you'll know how to create queries.

Know that you can only create SELECT queries, don't try creating any other queries like DESCRIBE, INSERT, etc.

DO NOT FORGET TO USE staging. schema for the tables that you get enlisted, please

**Your Task:**
Help analyze the Rust ecosystem. Suggest interesting questions to explore, write efficient queries, and explain findings in context."""
                    )
                )
            ]
        )


if __name__ == "__main__":
    mcp.run(transport="streamable-http")