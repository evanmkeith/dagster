from dagster_duckdb import DuckDBResource

import dagster as dg


database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE_URL") 
)

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
        }
    )
