"""
Jobs with different execution patterns for serverless Dagster+
"""

import json
from datetime import datetime, timedelta
from typing import Dict, Any, List

import dagster as dg
from dagster import (
    job, op, graph, GraphOut, In, Out, OpExecutionContext,
    RunConfig, Config, Selector, IntSource, StringSource,
    define_asset_job, RetryPolicy, Backoff, Jitter,
    DynamicOut, DynamicOutput, resource
)

# Note: collect functionality may vary by Dagster version - using simplified approach
from pydantic import Field

from .serverless_assets import (
    raw_customers, raw_orders, clean_customers, clean_orders,
    analytics_metrics,  # Multi-asset containing the individual metrics
    daily_sales, hourly_traffic, monthly_business_report,
    daily_partitions, hourly_partitions, business_units
)
from .resources import database, api_client, slack


# ============================================================================
# BASIC ASSET JOBS
# ============================================================================

# Simple asset selection job
data_ingestion_job = define_asset_job(
    name="data_ingestion",
    selection=[raw_customers, raw_orders],
    description="Ingest raw data from external APIs",
    tags={"team": "data", "priority": "high"}
)

# Asset job with dependencies
data_processing_job = define_asset_job(
    name="data_processing", 
    selection=[clean_customers, clean_orders],
    description="Clean and validate ingested data"
)

# Analytics job with multi-asset
analytics_job = define_asset_job(
    name="analytics",
    selection=[analytics_metrics],  # Multi-asset containing all analytics
    description="Generate customer and order analytics"
)

# End-to-end pipeline job
full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection="*",  # Select all assets
    description="Complete data pipeline from ingestion to reporting"
)


# ============================================================================
# PARTITIONED ASSET JOBS
# ============================================================================

# Daily partitioned job
daily_sales_job = define_asset_job(
    name="daily_sales_job",
    selection=[daily_sales],
    partitions_def=daily_partitions,
    description="Process daily sales data for specific partitions"
)

# Hourly partitioned job
hourly_traffic_job = define_asset_job(
    name="hourly_traffic_job", 
    selection=[hourly_traffic],
    partitions_def=hourly_partitions,
    description="Process hourly traffic data"
)

# Static partitioned job (business units)
department_job = define_asset_job(
    name="department_kpis_job",
    selection=["department_kpis"],  # Reference by name
    partitions_def=business_units,
    description="Calculate KPIs for each business unit"
)

# Multi-partition backfill job
backfill_job = define_asset_job(
    name="backfill_historical_data",
    selection=[daily_sales, hourly_traffic],
    description="Backfill historical data across multiple partitions"
)


# ============================================================================
# TRADITIONAL OP-BASED JOBS
# ============================================================================

class ProcessingConfig(Config):
    """Configuration for data processing operations"""
    batch_size: int = Field(
        default=100,
        description="Number of records to process in each batch"
    )
    enable_validation: bool = Field(
        default=True,
        description="Whether to enable data validation checks"
    )
    output_format: str = Field(
        default="json",
        description="Output format for processed data"
    )


@op(
    out={"processed_data": Out(Dict[str, Any])},
    description="Extract data from external source with configurable parameters"
)
def extract_data(context: OpExecutionContext) -> Dict[str, Any]:
    """Extract data from external source"""
    # Simplified version without config for demo compatibility
    batch_size = 50  # Default batch size
    
    context.log.info(f"Extracting data with batch size: {batch_size}")
    
    # Simulate data extraction
    extracted_data = {
        "records": [
            {"id": i, "value": f"data_{i}", "timestamp": datetime.now().isoformat()}
            for i in range(batch_size)
        ],
        "extraction_time": datetime.now().isoformat(),
        "batch_size": batch_size,
        "total_records": batch_size
    }
    
    context.add_output_metadata(
        output_name="processed_data",
        metadata={
            "num_records": len(extracted_data["records"]),
            "extraction_time": extracted_data["extraction_time"],
            "batch_size": batch_size
        }
    )
    
    return extracted_data


@op(
    ins={"raw_data": In(Dict[str, Any])},
    out={"transformed_data": Out(Dict[str, Any])},
    description="Transform extracted data with validation"
)
def transform_data(context: OpExecutionContext, raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform and validate data"""
    # Simplified version without config for demo compatibility
    enable_validation = True
    output_format = "json"
    
    context.log.info(f"Transforming {len(raw_data['records'])} records")
    
    # Simulate transformation logic
    transformed_records = []
    for record in raw_data["records"]:
        transformed_record = {
            "id": record["id"],
            "processed_value": f"processed_{record['value']}",
            "original_timestamp": record["timestamp"],
            "processed_timestamp": datetime.now().isoformat(),
            "is_valid": True  # Simplified validation
        }
        
        if enable_validation:
            # Simulate validation logic
            transformed_record["validation_passed"] = len(record["value"]) > 0
        
        transformed_records.append(transformed_record)
    
    transformed_data = {
        "records": transformed_records,
        "transformation_time": datetime.now().isoformat(),
        "validation_enabled": enable_validation,
        "output_format": output_format
    }
    
    context.add_output_metadata(
        output_name="transformed_data",
        metadata={
            "num_transformed": len(transformed_records),
            "validation_enabled": enable_validation,
            "output_format": output_format
        }
    )
    
    return transformed_data


@op(
    ins={"data": In(Dict[str, Any])},
    out={"load_result": Out(Dict[str, Any])},
    description="Load processed data to destination"
)
def load_data(context: OpExecutionContext, data: Dict[str, Any]) -> Dict[str, Any]:
    """Load data to destination"""
    context.log.info(f"Loading {len(data['records'])} records")
    
    # Simulate loading to database/storage
    load_result = {
        "loaded_at": datetime.now().isoformat(),
        "record_count": len(data["records"]),
        "status": "success",
        "destination": "production_database"
    }
    
    context.add_output_metadata(
        output_name="load_result",
        metadata={
            "records_loaded": len(data["records"]),
            "load_status": "success",
            "destination": "production_database"
        }
    )
    
    return load_result


@job(description="Traditional ETL pipeline using ops and graphs")
def etl_pipeline():
    """Traditional ETL pipeline job"""
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)


# ============================================================================
# DYNAMIC JOBS
# ============================================================================

@op(
    out=DynamicOut(str),
    description="Generate dynamic list of items to process"
)
def generate_dynamic_items(context: OpExecutionContext):
    """Generate dynamic list of items for processing"""
    
    # Simulate dynamic item generation (e.g., files to process, customers to analyze)
    items = [f"item_{i}" for i in range(5)]
    
    context.log.info(f"Generated {len(items)} dynamic items: {items}")
    
    for item in items:
        yield DynamicOutput(
            value=item,
            mapping_key=item.replace("_", "-")  # Sanitize for mapping key
        )


@op(
    ins={"item": In(str)},
    out={"result": Out(Dict[str, Any])},
    description="Process individual dynamic item"
)
def process_dynamic_item(context: OpExecutionContext, item: str) -> Dict[str, Any]:
    """Process an individual dynamic item"""
    context.log.info(f"Processing dynamic item: {item}")
    
    # Simulate processing logic
    result = {
        "item": item,
        "processed_at": datetime.now().isoformat(),
        "processing_duration_ms": 150,  # Mock duration
        "status": "completed"
    }
    
    context.add_output_metadata(
        output_name="result",
        metadata={
            "item_id": item,
            "processing_status": "completed"
        }
    )
    
    return result


@op(
    ins={"results": In(List[Dict[str, Any]])},
    out={"summary": Out(Dict[str, Any])},
    description="Collect and summarize dynamic processing results"
)
def summarize_dynamic_results(context: OpExecutionContext, results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize results from dynamic processing"""
    context.log.info(f"Summarizing {len(results)} dynamic processing results")
    
    summary = {
        "total_items": len(results),
        "successful_items": len([r for r in results if r["status"] == "completed"]),
        "summary_generated_at": datetime.now().isoformat(),
        "items_processed": [r["item"] for r in results]
    }
    
    context.add_output_metadata(
        output_name="summary",
        metadata={
            "total_processed": summary["total_items"],
            "success_rate": summary["successful_items"] / summary["total_items"]
        }
    )
    
    return summary


# Note: Dynamic job with collect functionality - simplified for demo compatibility
@job(description="Simplified dynamic job processing")
def dynamic_processing_job():
    """Simplified dynamic job - collect functionality varies by Dagster version"""
    # Simplified version without collect - full implementation would use:
    # dynamic_items = generate_dynamic_items()
    # processed_results = dynamic_items.map(process_dynamic_item)  
    # summarize_dynamic_results(collect(processed_results))
    pass


# ============================================================================
# JOBS WITH RETRY POLICIES
# ============================================================================

@op(
    out={"data": Out(Dict[str, Any])},
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=2.0,  # 2 second delay between retries
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS
    ),
    description="Unreliable operation that may fail and needs retries"
)
def unreliable_api_call(context: OpExecutionContext) -> Dict[str, Any]:
    """Simulate an unreliable API call that might fail"""
    
    import random
    
    # Simulate 70% success rate
    if random.random() < 0.3:
        context.log.warning("API call failed, will be retried")
        raise Exception("Simulated API failure")
    
    context.log.info("API call succeeded")
    
    return {
        "api_response": {"data": "success", "timestamp": datetime.now().isoformat()},
        "attempt_successful": True
    }


@op(
    ins={"api_data": In(Dict[str, Any])},
    out={"processed": Out(Dict[str, Any])},
    description="Process data from unreliable API"
)
def process_api_data(context: OpExecutionContext, api_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process data from API call"""
    
    result = {
        "original_data": api_data["api_response"],
        "processed_at": datetime.now().isoformat(),
        "processing_status": "completed"
    }
    
    return result


@job(description="Job with retry policies for handling failures")
def resilient_job():
    """Job demonstrating retry policies"""
    api_data = unreliable_api_call()
    process_api_data(api_data)


# ============================================================================
# COMPLEX GRAPH-BASED JOBS
# ============================================================================

@op(out={"dataset_a": Out(Dict[str, Any])})
def extract_dataset_a(context: OpExecutionContext) -> Dict[str, Any]:
    """Extract dataset A"""
    return {"name": "dataset_a", "records": 100, "extracted_at": datetime.now().isoformat()}


@op(out={"dataset_b": Out(Dict[str, Any])})  
def extract_dataset_b(context: OpExecutionContext) -> Dict[str, Any]:
    """Extract dataset B"""
    return {"name": "dataset_b", "records": 200, "extracted_at": datetime.now().isoformat()}


@op(
    ins={"dataset_a": In(Dict[str, Any]), "dataset_b": In(Dict[str, Any])},
    out={"merged_data": Out(Dict[str, Any])}
)
def merge_datasets(context: OpExecutionContext, dataset_a: Dict[str, Any], dataset_b: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two datasets"""
    merged = {
        "datasets": [dataset_a, dataset_b],
        "total_records": dataset_a["records"] + dataset_b["records"],
        "merged_at": datetime.now().isoformat()
    }
    
    context.add_output_metadata(
        output_name="merged_data",
        metadata={
            "total_records": merged["total_records"],
            "source_datasets": 2
        }
    )
    
    return merged


@op(
    ins={"data": In(Dict[str, Any])},
    out={"analysis": Out(Dict[str, Any])}
)
def analyze_data(context: OpExecutionContext, data: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze merged data"""
    analysis = {
        "total_records_analyzed": data["total_records"],
        "analysis_type": "comprehensive",
        "insights": ["insight_1", "insight_2", "insight_3"],
        "analyzed_at": datetime.now().isoformat()
    }
    
    return analysis


@op(
    ins={"analysis": In(Dict[str, Any])},
    out={"report": Out(Dict[str, Any])}
)
def generate_report(context: OpExecutionContext, analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Generate final report"""
    report = {
        "report_type": "data_analysis",
        "insights_count": len(analysis["insights"]),
        "records_analyzed": analysis["total_records_analyzed"],
        "generated_at": datetime.now().isoformat(),
        "status": "completed"
    }
    
    context.add_output_metadata(
        output_name="report", 
        metadata={
            "insights_generated": report["insights_count"],
            "analysis_completeness": 1.0
        }
    )
    
    return report


@graph(
    description="Complex data processing graph with multiple parallel paths"
)
def complex_data_graph():
    """Complex graph with parallel extraction and downstream processing"""
    dataset_a = extract_dataset_a()
    dataset_b = extract_dataset_b()
    merged_data = merge_datasets(dataset_a, dataset_b)
    analysis = analyze_data(merged_data)
    return generate_report(analysis)


# Convert graph to job
complex_processing_job = complex_data_graph.to_job(
    name="complex_processing_job",
    description="Complex job demonstrating graph-based execution"
)