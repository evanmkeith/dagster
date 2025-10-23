"""
Dagster Serverless+ Comprehensive Demo
=====================================

This package demonstrates a comprehensive set of Dagster+ serverless features including:

- 🏗️  Various asset patterns (basic, multi-asset, partitioned, external)
- ⏰ Schedules and sensors for automation
- 🔧 Different job execution patterns (asset jobs, op-based, dynamic, graph-based)
- ✅ Asset checks for data quality and monitoring
- 🔌 Advanced I/O managers and resource configurations
- 📊 Comprehensive partitioning strategies and backfills
- 📈 Observability and monitoring features

To use this demo:

1. Start the Dagster UI:
   ```
   dagster-webserver -m dagster_quickstart
   ```

2. Or run specific jobs:
   ```
   dagster job execute -m dagster_quickstart -j data_ingestion_job
   ```

3. Materialize assets:
   ```
   dagster asset materialize -m dagster_quickstart --select raw_customers
   ```

Features demonstrated:
- Time-based partitions (daily, hourly, weekly, monthly)  
- Static partitions (regions, categories, business units)
- Multi-dimensional partitions (time x region x category)
- Asset checks for data quality validation
- Multiple I/O manager patterns (S3, warehouse, caching)
- Retry policies and error handling
- Monitoring and observability patterns
- Comprehensive backfill strategies
"""

from dagster_quickstart.defs.serverless_definitions import defs

__all__ = ["defs"]