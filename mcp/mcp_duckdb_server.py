import asyncio
import duckdb
import os
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import \
    Tool, \
    TextContent, \
    Resource, \
    Prompt, \
    PromptMessage, \
    GetPromptResult
import logging

logging.basicConfig(
    level=logging.DEBUG,
    filename=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output.log'),
    format='%(pathname)s - %(message)s'
)

app = Server("duckdb-crates")

def get_crates_duckdb_path():
    project_root_directory = os.path.dirname(os.path.dirname(__file__))
    data_folder = "data"
    duckdb_instance = "crates.duckdb"

    full_duckdb_path_instance = os.path.join(project_root_directory, data_folder, duckdb_instance)

    return full_duckdb_path_instance


@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="query_duckdb",
            description="Execute a read-only SQL query against the crates.io DuckDB database",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SELECT query to execute"
                    }
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="list_tables",
            description="List all tables in the staging schema",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    crates_duckdb_path = get_crates_duckdb_path()
    conn = duckdb.connect(crates_duckdb_path, read_only=True)
    
    try:
        if name == "list_tables":
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
        
        elif name == "query_duckdb":
            sql = arguments["sql"]
            
            # Safety check
            if not sql.strip().upper().startswith('SELECT'):
                return [TextContent(
                    type="text",
                    text="Error: Only SELECT queries are allowed"
                )]
            
            result = conn.execute(sql).fetchdf()
            
            return [TextContent(
                type="text",
                text=f"{result.to_markdown(index=False)}"
            )]
    
    except Exception as e:
        return [TextContent(
            type="text",
            text=f"Error: {str(e)}"
        )]
    finally:
        conn.close()

@app.list_resources()
async def list_resources() -> list[Resource]:
    return [
        Resource(
            uri="schema://staging/tables",
            name="Staging Schema Definition",
            mimeType="text/plain",
            description="Column definitions and relationships for all staging tables"
        )
    ]


@app.read_resource()
async def read_resource(uri: str) -> str:
    if str(uri) == "schema://staging/tables":
        # Path to your dbt schema files
        project_root = os.path.dirname(os.path.dirname(__file__))
        schema_path = os.path.join(project_root, "transformations", "models", "staging")
        
        # Read all .yml files in staging folder
        schema_content = []
        for yml_file in os.listdir(schema_path):
            if yml_file.endswith('.yml'):
                with open(os.path.join(schema_path, yml_file), 'r') as f:
                    schema_content.append(f"# {yml_file}\n{f.read()}\n")
        
        return "\n".join(schema_content)
    
    raise ValueError(f"Unknown resource: {uri}")


@app.list_prompts()
async def list_prompts() -> list[Prompt]:
    return [
        Prompt(
            name="setup-rust-crates-analytics",
            description="Load context about the Rust Crates Analytics project",
            arguments=[]
        )
    ]


@app.get_prompt()
async def get_prompt(name: str, arguments: dict) -> GetPromptResult:
    if name == "setup-rust-crates-analytics":
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

**Your Task:**
Help analyze the Rust ecosystem. Suggest interesting questions to explore, write efficient queries, and explain findings in context."""
                    )
                )
            ]
        )
    
    raise ValueError(f"Unknown prompt: {name}")

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())