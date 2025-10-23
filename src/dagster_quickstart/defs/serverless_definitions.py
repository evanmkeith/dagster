"""
Comprehensive serverless Dagster+ definitions integrating all features
"""

import dagster as dg
from dagster import Definitions

# Import all assets
from .serverless_assets import (
    raw_customers, raw_orders, clean_customers, clean_orders,
    analytics_metrics,  # This is the multi-asset that contains customer_metrics, order_metrics, combined_summary
    daily_sales, hourly_traffic, department_kpis,
    monthly_business_report, payment_reconciliation,
    configurable_report, external_stripe_data, external_salesforce_data
)

from .partitions_backfill import (
    daily_transactions, hourly_system_metrics, weekly_business_summary,
    monthly_financial_report, regional_sales_performance, category_performance,
    daily_regional_sales, complex_sales_analysis, partition_health_monitor
)

from .asset_checks import (
    raw_customers_record_count_check, raw_customers_freshness_check,
    raw_orders_duplicate_check, clean_customers_quality_check,
    clean_orders_anomaly_check, daily_sales_completeness_check,
    data_quality_metrics, pipeline_performance_metrics,
    customer_engagement_metrics
)

# Import schedules and sensors
from .schedules_sensors import (
    daily_data_refresh_schedule, monthly_reporting_schedule,
    daily_sales_schedule, data_quality_sensor, file_arrival_sensor,
    partition_reconciliation_sensor, business_hours_sensor,
    analytics_refresh_sensor, configurable_alert_sensor
)

# Import jobs
from .jobs import (
    data_ingestion_job, data_processing_job, analytics_job, full_pipeline_job,
    daily_sales_job, hourly_traffic_job, department_job, backfill_job,
    etl_pipeline, dynamic_processing_job, resilient_job,
    complex_processing_job
)

from .partitions_backfill import (
    daily_backfill_job, multi_dimensional_backfill_job, 
    static_backfill_job, full_backfill_job,
    daily_processing_schedule
)

# Import all jobs from schedules_sensors
from .schedules_sensors import (
    daily_data_refresh_job, business_reporting_job, data_quality_check_job,
    file_processing_job, cross_partition_reconciliation_job,
    business_hours_processing_job, analytics_refresh_job,
    configurable_alert_job
)

# Import resources
from .resources import (
    # Original resources
    database_resource, s3_io_manager, database, api_client, slack,
    # Enhanced resources  
    serverless_s3_io_manager, warehouse_io_manager, redis_cache_manager,
    advanced_api_client, enhanced_slack
)

# Import dbt assets
from .dbt_assets import dbt_assets_list, dbt_resources

# Collect all assets
all_assets = [
    # Basic serverless assets
    raw_customers, raw_orders, clean_customers, clean_orders,
    analytics_metrics,  # Multi-asset containing customer_metrics, order_metrics, combined_summary
    daily_sales, hourly_traffic, department_kpis,
    monthly_business_report, payment_reconciliation,
    configurable_report,
    
    # Partition examples
    daily_transactions, hourly_system_metrics, weekly_business_summary,
    monthly_financial_report, regional_sales_performance, category_performance,
    daily_regional_sales, complex_sales_analysis, 
    
    # Observability assets
    data_quality_metrics, pipeline_performance_metrics,
    customer_engagement_metrics, partition_health_monitor,
    
    # External assets
    external_stripe_data, external_salesforce_data
] + dbt_assets_list  # Add dbt assets

# Collect all asset checks (excluding ones that reference multi-asset outputs)
all_asset_checks = [
    raw_customers_record_count_check, raw_customers_freshness_check,
    raw_orders_duplicate_check, clean_customers_quality_check,
    clean_orders_anomaly_check, daily_sales_completeness_check
]

# Collect all schedules
all_schedules = [
    daily_data_refresh_schedule, monthly_reporting_schedule,
    daily_sales_schedule, daily_processing_schedule
]

# Collect all sensors  
all_sensors = [
    data_quality_sensor, file_arrival_sensor, partition_reconciliation_sensor,
    business_hours_sensor, analytics_refresh_sensor, configurable_alert_sensor
]

# Collect all jobs
all_jobs = [
    # Asset jobs (only include ones that don't reference problematic assets)
    data_ingestion_job, data_processing_job, full_pipeline_job,
    daily_sales_job, hourly_traffic_job, department_job, backfill_job,
    
    # Backfill jobs
    daily_backfill_job, multi_dimensional_backfill_job,
    static_backfill_job, full_backfill_job,
    
    # Op-based jobs
    etl_pipeline, dynamic_processing_job, resilient_job, complex_processing_job,
    
    # Sensor/schedule triggered jobs
    daily_data_refresh_job, business_reporting_job, data_quality_check_job,
    file_processing_job, cross_partition_reconciliation_job,
    business_hours_processing_job, analytics_refresh_job, configurable_alert_job
]

# All resources
all_resources = {
    # Original resources
    "duckdb": database_resource,
    "s3_io_manager": s3_io_manager, 
    "database": database,
    "api_client": api_client,
    "slack": slack,
    
    # Enhanced serverless resources
    "serverless_s3_io_manager": serverless_s3_io_manager,
    "warehouse_io_manager": warehouse_io_manager,
    "redis_cache_manager": redis_cache_manager,
    "advanced_api_client": advanced_api_client,
    "enhanced_slack": enhanced_slack,
    
    # Add dbt resources
    **dbt_resources
}

# Main definitions - single Definitions object for dg dev compatibility
defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    schedules=all_schedules,
    sensors=all_sensors,
    jobs=all_jobs,
    resources=all_resources
)