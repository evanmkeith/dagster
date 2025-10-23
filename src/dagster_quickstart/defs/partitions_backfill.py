"""
Comprehensive partitioning and backfill examples for serverless Dagster+
"""

from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Union
import pandas as pd

import dagster as dg
from dagster import (
    asset, multi_asset, AssetOut, AssetExecutionContext, Config,
    DailyPartitionsDefinition, HourlyPartitionsDefinition, WeeklyPartitionsDefinition,
    MonthlyPartitionsDefinition, StaticPartitionsDefinition, MultiPartitionsDefinition,
    TimeWindowPartitionsDefinition, 
    MultiPartitionKey, AssetMaterialization, MetadataValue,
    define_asset_job, AssetSelection,
    build_schedule_from_partitioned_job, ScheduleEvaluationContext
)
from pydantic import Field

from .resources import database, api_client, slack


# ============================================================================
# TIME-BASED PARTITIONS
# ============================================================================

# Daily partitions starting from specific date
daily_partitions = DailyPartitionsDefinition(
    start_date="2024-01-01",
    end_date="2024-12-31",  # Optional end date
    timezone="UTC",
    fmt="%Y-%m-%d"
)

@asset(
    partitions_def=daily_partitions,
    group_name="time_partitioned",
    description="Daily transaction data with comprehensive partitioning"
)
def daily_transactions(context: AssetExecutionContext, database: database.__class__) -> pd.DataFrame:
    """Generate daily transaction data for each partition"""
    partition_date = context.partition_key
    context.log.info(f"Processing transactions for {partition_date}")
    
    # Simulate loading transactions for specific date
    date_obj = datetime.strptime(partition_date, "%Y-%m-%d")
    
    # Generate mock transactions based on day of year (seasonal patterns)
    day_of_year = date_obj.timetuple().tm_yday
    base_transactions = 100 + int(50 * (1 + 0.5 * (day_of_year % 365) / 365))
    
    transactions = pd.DataFrame({
        "transaction_id": range(1, base_transactions + 1),
        "date": [partition_date] * base_transactions,
        "amount": [round(100 + 50 * (i % 10), 2) for i in range(base_transactions)],
        "customer_id": [(i % 20) + 1 for i in range(base_transactions)],
        "category": [["food", "retail", "entertainment", "transport", "utilities"][i % 5] for i in range(base_transactions)]
    })
    
    context.add_output_metadata({
        "partition_date": partition_date,
        "transaction_count": len(transactions),
        "total_amount": float(transactions["amount"].sum()),
        "unique_customers": transactions["customer_id"].nunique(),
        "categories": list(transactions["category"].unique())
    })
    
    return transactions


# Hourly partitions for high-frequency data
hourly_partitions = HourlyPartitionsDefinition(
    start_date="2024-01-01-00:00",
    timezone="UTC",
    fmt="%Y-%m-%d-%H:%M"
)

@asset(
    partitions_def=hourly_partitions,
    group_name="time_partitioned",
    description="Hourly system metrics for monitoring"
)
def hourly_system_metrics(context: AssetExecutionContext) -> Dict[str, Any]:
    """Track system metrics on hourly basis"""
    partition_hour = context.partition_key
    context.log.info(f"Collecting metrics for hour: {partition_hour}")
    
    # Parse hour for time-based patterns
    hour_dt = datetime.strptime(partition_hour, "%Y-%m-%d-%H:%M")
    hour_of_day = hour_dt.hour
    day_of_week = hour_dt.weekday()
    
    # Simulate metrics with realistic patterns
    business_hours_multiplier = 1.5 if 9 <= hour_of_day <= 17 else 0.7
    weekend_multiplier = 0.6 if day_of_week >= 5 else 1.0
    
    base_cpu = 30 * business_hours_multiplier * weekend_multiplier
    base_memory = 60 * business_hours_multiplier * weekend_multiplier
    
    metrics = {
        "hour": partition_hour,
        "cpu_usage_percent": round(base_cpu + (hour_of_day % 10), 1),
        "memory_usage_percent": round(base_memory + (hour_of_day % 15), 1),
        "disk_usage_gb": round(100 + hour_of_day * 2, 1),
        "network_throughput_mbps": round(50 + (hour_of_day % 20), 1),
        "active_connections": int(100 + 20 * business_hours_multiplier * weekend_multiplier),
        "response_time_ms": round(50 + (100 - base_cpu) / 2, 1),
        "error_rate_percent": round(max(0.1, 2 - business_hours_multiplier), 2)
    }
    
    context.add_output_metadata({
        "hour": partition_hour,
        "cpu_usage": metrics["cpu_usage_percent"],
        "memory_usage": metrics["memory_usage_percent"],
        "error_rate": metrics["error_rate_percent"],
        "is_business_hours": 9 <= hour_of_day <= 17,
        "is_weekend": day_of_week >= 5
    })
    
    return metrics


# Weekly partitions for aggregated reporting
weekly_partitions = WeeklyPartitionsDefinition(
    start_date="2024-01-01",
    timezone="UTC"
)

@asset(
    partitions_def=weekly_partitions,
    group_name="time_partitioned",
    description="Weekly business summary reports"
)
def weekly_business_summary(context: AssetExecutionContext, database: database.__class__) -> Dict[str, Any]:
    """Generate weekly business summary for each partition"""
    partition_week = context.partition_key
    context.log.info(f"Generating weekly summary for {partition_week}")
    
    # Parse week partition (format is typically YYYY-MM-DD for start of week)
    week_start = datetime.strptime(partition_week, "%Y-%m-%d")
    week_end = week_start + timedelta(days=6)
    
    # Simulate weekly business metrics
    week_num = week_start.isocalendar()[1]  # Get week number for calculations
    summary = {
        "week": partition_week,
        "week_start_date": week_start.strftime("%Y-%m-%d"),
        "week_end_date": week_end.strftime("%Y-%m-%d"),
        "total_revenue": round(10000 + (week_num % 20) * 500, 2),
        "total_transactions": 800 + (week_num % 15) * 50,
        "active_customers": 150 + (week_num % 10) * 10,
        "new_customers": 20 + (week_num % 8) * 2,
        "avg_transaction_value": 0,  # Will calculate
        "customer_satisfaction": round(4.2 + (week_num % 5) * 0.1, 1)
    }
    
    # Calculate derived metrics
    if summary["total_transactions"] > 0:
        summary["avg_transaction_value"] = round(summary["total_revenue"] / summary["total_transactions"], 2)
    
    context.add_output_metadata({
        "week": partition_week,
        "date_range": f"{summary['week_start_date']} to {summary['week_end_date']}",
        "revenue": summary["total_revenue"],
        "transactions": summary["total_transactions"],
        "customer_satisfaction": summary["customer_satisfaction"]
    })
    
    return summary


# Monthly partitions for historical analysis
monthly_partitions = MonthlyPartitionsDefinition(
    start_date="2024-01-01",
    timezone="UTC"
)

@asset(
    partitions_def=monthly_partitions,
    group_name="time_partitioned",
    description="Monthly financial reports and KPIs"
)
def monthly_financial_report(context: AssetExecutionContext) -> Dict[str, Any]:
    """Generate comprehensive monthly financial reports"""
    partition_month = context.partition_key
    context.log.info(f"Generating financial report for {partition_month}")
    
    # Parse the month partition (format is YYYY-MM-DD for start of month)
    month_start = datetime.strptime(partition_month, "%Y-%m-%d")
    year = month_start.year
    month = month_start.month
    month_name = month_start.strftime("%B %Y")
    
    # Days in month for calculations
    import calendar
    days_in_month = calendar.monthrange(year, month)[1]
    
    # Simulate monthly financial data
    base_revenue = 300000 + (month % 12) * 25000  # Seasonal patterns
    
    report = {
        "month": partition_month,
        "month_name": month_name,
        "days_in_month": days_in_month,
        "financial_metrics": {
            "total_revenue": base_revenue,
            "total_expenses": round(base_revenue * 0.7, 2),
            "gross_profit": round(base_revenue * 0.3, 2),
            "operating_expenses": round(base_revenue * 0.15, 2),
            "net_profit": round(base_revenue * 0.15, 2),
            "profit_margin": 15.0
        },
        "customer_metrics": {
            "total_customers": 2000 + month * 50,
            "new_customers": 150 + month * 10,
            "churned_customers": 80 + month * 5,
            "customer_lifetime_value": round(base_revenue / (2000 + month * 50), 2)
        },
        "operational_metrics": {
            "total_orders": int(base_revenue / 125),  # Avg order value ~$125
            "fulfillment_rate": round(96.5 + (month % 3), 1),
            "return_rate": round(3.2 - (month % 4) * 0.1, 1),
            "customer_satisfaction": round(4.3 + (month % 6) * 0.05, 1)
        }
    }
    
    context.add_output_metadata({
        "month": month_name,
        "revenue": report["financial_metrics"]["total_revenue"],
        "profit_margin": report["financial_metrics"]["profit_margin"],
        "new_customers": report["customer_metrics"]["new_customers"],
        "fulfillment_rate": report["operational_metrics"]["fulfillment_rate"]
    })
    
    return report


# ============================================================================
# STATIC PARTITIONS
# ============================================================================

# Geographic regions
regions_partitions = StaticPartitionsDefinition([
    "north_america", "europe", "asia_pacific", "latin_america", "middle_east_africa"
])

@asset(
    partitions_def=regions_partitions,
    group_name="geographic_partitioned", 
    description="Regional sales performance by geographic region"
)
def regional_sales_performance(context: AssetExecutionContext) -> Dict[str, Any]:
    """Calculate sales performance metrics by region"""
    region = context.partition_key
    context.log.info(f"Analyzing sales performance for region: {region}")
    
    # Regional characteristics for simulation
    region_data = {
        "north_america": {
            "currency": "USD",
            "avg_order_value": 150,
            "customer_base": 10000,
            "growth_rate": 12.5,
            "languages": ["en"]
        },
        "europe": {
            "currency": "EUR", 
            "avg_order_value": 130,
            "customer_base": 8500,
            "growth_rate": 8.3,
            "languages": ["en", "de", "fr", "es", "it"]
        },
        "asia_pacific": {
            "currency": "USD",
            "avg_order_value": 95,
            "customer_base": 15000,
            "growth_rate": 18.7,
            "languages": ["en", "zh", "ja", "ko"]
        },
        "latin_america": {
            "currency": "USD",
            "avg_order_value": 85,
            "customer_base": 3500,
            "growth_rate": 22.1,
            "languages": ["es", "pt"]
        },
        "middle_east_africa": {
            "currency": "USD", 
            "avg_order_value": 110,
            "customer_base": 2000,
            "growth_rate": 15.8,
            "languages": ["en", "ar", "fr"]
        }
    }
    
    base_data = region_data.get(region, region_data["north_america"])
    customer_base = base_data["customer_base"]
    avg_order_value = base_data["avg_order_value"]
    
    # Calculate metrics
    monthly_orders = int(customer_base * 0.15)  # 15% of customers order monthly
    monthly_revenue = monthly_orders * avg_order_value
    
    performance = {
        "region": region,
        "reporting_date": datetime.now().strftime("%Y-%m-%d"),
        "regional_characteristics": base_data,
        "performance_metrics": {
            "monthly_revenue": monthly_revenue,
            "monthly_orders": monthly_orders,
            "active_customers": customer_base,
            "avg_order_value": avg_order_value,
            "customer_acquisition_cost": round(avg_order_value * 0.2, 2),
            "customer_lifetime_value": round(avg_order_value * 8.5, 2),
            "conversion_rate": round(3.2 + (len(region) % 3), 1),
            "market_penetration": round(customer_base / 50000 * 100, 2)
        },
        "growth_metrics": {
            "yoy_growth_rate": base_data["growth_rate"],
            "customer_growth_rate": round(base_data["growth_rate"] * 0.8, 1),
            "revenue_growth_rate": round(base_data["growth_rate"] * 1.1, 1)
        }
    }
    
    context.add_output_metadata({
        "region": region,
        "currency": base_data["currency"],
        "monthly_revenue": monthly_revenue,
        "customer_base": customer_base,
        "growth_rate": base_data["growth_rate"],
        "languages_supported": len(base_data["languages"])
    })
    
    return performance


# Product categories
categories_partitions = StaticPartitionsDefinition([
    "electronics", "clothing", "books", "home_garden", "sports_outdoors", 
    "beauty_health", "toys_games", "automotive", "food_beverage"
])

@asset(
    partitions_def=categories_partitions,
    group_name="product_partitioned",
    description="Product category performance analysis"
)
def category_performance(context: AssetExecutionContext) -> Dict[str, Any]:
    """Analyze performance metrics by product category"""
    category = context.partition_key
    context.log.info(f"Analyzing performance for category: {category}")
    
    # Category characteristics
    category_data = {
        "electronics": {"margin": 8.5, "return_rate": 5.2, "avg_rating": 4.1, "seasonal": False},
        "clothing": {"margin": 45.0, "return_rate": 15.8, "avg_rating": 3.9, "seasonal": True},
        "books": {"margin": 25.0, "return_rate": 2.1, "avg_rating": 4.3, "seasonal": False},
        "home_garden": {"margin": 35.0, "return_rate": 8.7, "avg_rating": 4.0, "seasonal": True},
        "sports_outdoors": {"margin": 28.0, "return_rate": 6.5, "avg_rating": 4.2, "seasonal": True},
        "beauty_health": {"margin": 55.0, "return_rate": 12.3, "avg_rating": 3.8, "seasonal": False},
        "toys_games": {"margin": 40.0, "return_rate": 4.8, "avg_rating": 4.4, "seasonal": True},
        "automotive": {"margin": 18.0, "return_rate": 7.9, "avg_rating": 4.0, "seasonal": False},
        "food_beverage": {"margin": 22.0, "return_rate": 3.2, "avg_rating": 4.1, "seasonal": False}
    }
    
    base_data = category_data.get(category, {"margin": 25.0, "return_rate": 8.0, "avg_rating": 4.0, "seasonal": False})
    
    # Simulate performance data
    base_sales = 50000 + hash(category) % 30000  # Deterministic but varied
    
    performance = {
        "category": category,
        "analysis_date": datetime.now().strftime("%Y-%m-%d"),
        "sales_metrics": {
            "monthly_sales_units": base_sales,
            "monthly_revenue": round(base_sales * (80 + hash(category) % 40), 2),
            "avg_selling_price": round(80 + hash(category) % 40, 2),
            "gross_margin_percent": base_data["margin"],
            "return_rate_percent": base_data["return_rate"]
        },
        "customer_metrics": {
            "avg_rating": base_data["avg_rating"],
            "review_count": base_sales // 10,
            "repeat_purchase_rate": round(25 + base_data["margin"] / 2, 1),
            "customer_satisfaction": round(base_data["avg_rating"] * 20, 1)
        },
        "inventory_metrics": {
            "inventory_turnover": round(12 - base_data["return_rate"] / 2, 1),
            "stockout_rate": round(max(1.0, 8 - base_data["margin"] / 5), 1),
            "overstock_rate": round(base_data["return_rate"] / 2, 1)
        },
        "category_attributes": {
            "is_seasonal": base_data["seasonal"],
            "profit_margin_category": "high" if base_data["margin"] > 30 else "medium" if base_data["margin"] > 20 else "low",
            "return_risk": "high" if base_data["return_rate"] > 10 else "medium" if base_data["return_rate"] > 5 else "low"
        }
    }
    
    context.add_output_metadata({
        "category": category,
        "monthly_revenue": performance["sales_metrics"]["monthly_revenue"],
        "gross_margin": base_data["margin"],
        "avg_rating": base_data["avg_rating"],
        "return_rate": base_data["return_rate"],
        "is_seasonal": base_data["seasonal"]
    })
    
    return performance


# ============================================================================
# MULTI-DIMENSIONAL PARTITIONS
# ============================================================================

# Multi-dimensional partitioning: Time x Region
time_region_partitions = MultiPartitionsDefinition({
    "date": DailyPartitionsDefinition(start_date="2024-01-01", end_date="2024-03-31"),
    "region": StaticPartitionsDefinition(["us_east", "us_west", "eu", "asia"])
})

@asset(
    partitions_def=time_region_partitions,
    group_name="multi_dimensional",
    description="Sales data partitioned by both date and region"
)
def daily_regional_sales(context: AssetExecutionContext) -> Dict[str, Any]:
    """Track daily sales performance across different regions"""
    partition_key = context.partition_key
    
    if isinstance(partition_key, MultiPartitionKey):
        date_key = partition_key.keys_by_dimension["date"]
        region_key = partition_key.keys_by_dimension["region"]
    else:
        # Fallback for string representation
        date_key, region_key = str(partition_key).split("|")
    
    context.log.info(f"Processing sales for {date_key} in region {region_key}")
    
    # Regional multipliers
    region_multipliers = {
        "us_east": 1.2,
        "us_west": 1.1, 
        "eu": 0.9,
        "asia": 1.3
    }
    
    # Day of week patterns
    date_obj = datetime.strptime(date_key, "%Y-%m-%d")
    weekday = date_obj.weekday()
    weekend_multiplier = 0.7 if weekday >= 5 else 1.0
    
    # Calculate sales metrics
    base_sales = 1000
    region_multiplier = region_multipliers.get(region_key, 1.0)
    final_sales = int(base_sales * region_multiplier * weekend_multiplier)
    
    sales_data = {
        "date": date_key,
        "region": region_key,
        "sales_units": final_sales,
        "revenue": round(final_sales * 45.50, 2),  # $45.50 avg price
        "unique_customers": int(final_sales * 0.8),  # 80% unique customers
        "avg_order_value": 45.50,
        "regional_metrics": {
            "region_multiplier": region_multiplier,
            "weekend_adjustment": weekend_multiplier,
            "is_weekend": weekday >= 5,
            "day_of_week": date_obj.strftime("%A")
        }
    }
    
    context.add_output_metadata({
        "partition_date": date_key,
        "partition_region": region_key,
        "sales_units": final_sales,
        "revenue": sales_data["revenue"],
        "is_weekend": weekday >= 5,
        "region_performance": region_multiplier
    })
    
    return sales_data


# Time x Category multi-partition (Dagster only supports 2 dimensions)
complex_partitions = MultiPartitionsDefinition({
    "month": MonthlyPartitionsDefinition(start_date="2024-01-01"),
    "category": StaticPartitionsDefinition(["premium", "standard", "budget"])
})

@asset(
    partitions_def=complex_partitions,
    group_name="multi_dimensional",
    description="Complex multi-dimensional sales analysis by month and category"
)
def complex_sales_analysis(context: AssetExecutionContext) -> Dict[str, Any]:
    """Comprehensive sales analysis across month and category"""
    partition_key = context.partition_key
    
    if isinstance(partition_key, MultiPartitionKey):
        month_key = partition_key.keys_by_dimension["month"]
        category_key = partition_key.keys_by_dimension["category"]
    else:
        # Parse string representation
        keys = str(partition_key).split("|")
        month_key, category_key = keys
    
    # Default channel for analysis
    channel_key = "online"  # Simplified to single channel
    
    context.log.info(f"Analyzing {category_key} sales for {month_key}")
    
    # Category characteristics
    category_multipliers = {
        "premium": {"price": 200, "margin": 0.4, "volume": 0.3},
        "standard": {"price": 100, "margin": 0.25, "volume": 1.0},
        "budget": {"price": 50, "margin": 0.15, "volume": 1.8}
    }
    
    # Channel characteristics
    channel_multipliers = {
        "online": {"cost": 0.12, "conversion": 0.15, "reach": 1.5},
        "retail": {"cost": 0.25, "conversion": 0.08, "reach": 1.0},
        "wholesale": {"cost": 0.08, "conversion": 0.25, "reach": 0.6}
    }
    
    category_data = category_multipliers.get(category_key, category_multipliers["standard"])
    channel_data = channel_multipliers.get(channel_key, channel_multipliers["online"])
    
    # Calculate metrics
    base_volume = 1000
    volume = int(base_volume * category_data["volume"] * channel_data["reach"])
    revenue = volume * category_data["price"]
    gross_profit = revenue * category_data["margin"]
    marketing_cost = revenue * channel_data["cost"]
    net_profit = gross_profit - marketing_cost
    
    analysis = {
        "month": month_key,
        "category": category_key,
        "channel": channel_key,  # Default channel
        "performance_metrics": {
            "units_sold": volume,
            "revenue": round(revenue, 2),
            "gross_profit": round(gross_profit, 2),
            "marketing_cost": round(marketing_cost, 2),
            "net_profit": round(net_profit, 2),
            "gross_margin": category_data["margin"],
            "net_margin": round(net_profit / revenue, 3) if revenue > 0 else 0
        },
        "efficiency_metrics": {
            "cost_per_acquisition": round(marketing_cost / volume, 2) if volume > 0 else 0,
            "return_on_ad_spend": round(revenue / marketing_cost, 2) if marketing_cost > 0 else 0,
            "profit_per_unit": round(net_profit / volume, 2) if volume > 0 else 0
        },
        "segment_analysis": {
            "category_performance": category_data,
            "channel_performance": channel_data,
            "segment_combination": f"{category_key}_{channel_key}"
        }
    }
    
    context.add_output_metadata({
        "month": month_key,
        "category": category_key,
        "channel": channel_key,
        "revenue": analysis["performance_metrics"]["revenue"],
        "net_margin": analysis["performance_metrics"]["net_margin"],
        "units_sold": volume,
        "roas": analysis["efficiency_metrics"]["return_on_ad_spend"]
    })
    
    return analysis


# ============================================================================
# BACKFILL JOBS AND STRATEGIES
# ============================================================================

# Job for daily backfilling
daily_backfill_job = define_asset_job(
    name="daily_backfill_job",
    selection=AssetSelection.assets(daily_transactions),
    partitions_def=daily_partitions,
    description="Backfill daily transaction data"
)

# Job for multi-dimensional backfilling
multi_dimensional_backfill_job = define_asset_job(
    name="multi_dimensional_backfill_job", 
    selection=AssetSelection.assets(daily_regional_sales, complex_sales_analysis),
    description="Backfill multi-dimensional partitioned assets"
)

# Job for static partition backfilling
static_backfill_job = define_asset_job(
    name="static_backfill_job",
    selection=AssetSelection.assets(regional_sales_performance, category_performance),
    description="Process all static partitions (regions, categories)"
)

# Comprehensive backfill job
full_backfill_job = define_asset_job(
    name="comprehensive_backfill_job",
    selection=AssetSelection.groups("time_partitioned", "geographic_partitioned", "product_partitioned", "multi_dimensional"),
    description="Comprehensive backfill across all partition types"
)


# ============================================================================
# PARTITION-AWARE SCHEDULES
# ============================================================================

# Daily processing schedule (automatically derived from partition definition)
daily_processing_schedule = build_schedule_from_partitioned_job(
    job=daily_backfill_job,
    name="daily_processing_schedule",
    description="Daily processing of transaction data"
)

# Weekly batch processing - commented out as corresponding job doesn't exist
# @dg.schedule(
#     job_name="weekly_batch_processing",
#     cron_schedule="0 6 * * 1",  # Monday 6 AM
#     description="Weekly batch processing for time-sensitive partitions"
# )
# def weekly_batch_schedule(context: ScheduleEvaluationContext):
#     """Schedule for weekly batch processing"""
#     # Process last week's data
#     last_week = context.scheduled_execution_time - timedelta(days=7)
#     
#     return dg.RunConfig(
#         tags={
#             "batch_type": "weekly",
#             "processing_week": last_week.strftime("%Y-W%U"),
#             "scheduled_time": context.scheduled_execution_time.isoformat()
#         }
#     )


# ============================================================================
# PARTITION UTILITIES AND HELPERS
# ============================================================================

@asset(
    group_name="utilities",
    description="Partition health monitoring and metadata"
)
def partition_health_monitor(context: AssetExecutionContext) -> Dict[str, Any]:
    """Monitor health and status of all partitioned assets"""
    
    current_time = datetime.now()
    
    # Simulate partition health monitoring
    partition_health = {
        "monitoring_timestamp": current_time.isoformat(),
        "daily_partitions": {
            "total_expected": 90,  # 3 months
            "materialized": 87,
            "failed": 2,
            "missing": 1,
            "success_rate": 96.7
        },
        "hourly_partitions": {
            "total_expected": 168,  # 1 week
            "materialized": 165,
            "failed": 1, 
            "missing": 2,
            "success_rate": 98.2
        },
        "static_partitions": {
            "regions": {"total": 5, "materialized": 5, "success_rate": 100.0},
            "categories": {"total": 9, "materialized": 8, "success_rate": 88.9}
        },
        "multi_dimensional": {
            "daily_regional": {"total_combinations": 120, "materialized": 115, "success_rate": 95.8},
            "complex_analysis": {"total_combinations": 108, "materialized": 102, "success_rate": 94.4}
        }
    }
    
    # Calculate overall health score
    all_success_rates = [
        partition_health["daily_partitions"]["success_rate"],
        partition_health["hourly_partitions"]["success_rate"],
        partition_health["static_partitions"]["regions"]["success_rate"],
        partition_health["static_partitions"]["categories"]["success_rate"],
        partition_health["multi_dimensional"]["daily_regional"]["success_rate"],
        partition_health["multi_dimensional"]["complex_analysis"]["success_rate"]
    ]
    
    overall_health = sum(all_success_rates) / len(all_success_rates)
    partition_health["overall_health_score"] = round(overall_health, 1)
    
    # Determine health status
    if overall_health >= 98:
        health_status = "excellent"
    elif overall_health >= 95:
        health_status = "good" 
    elif overall_health >= 90:
        health_status = "fair"
    else:
        health_status = "poor"
    
    partition_health["health_status"] = health_status
    
    context.add_output_metadata({
        "overall_health_score": overall_health,
        "health_status": health_status,
        "total_partitions_monitored": sum([
            partition_health["daily_partitions"]["total_expected"],
            partition_health["hourly_partitions"]["total_expected"],
            partition_health["static_partitions"]["regions"]["total"],
            partition_health["static_partitions"]["categories"]["total"],
            partition_health["multi_dimensional"]["daily_regional"]["total_combinations"],
            partition_health["multi_dimensional"]["complex_analysis"]["total_combinations"]
        ])
    })
    
    context.log.info(f"📊 Overall partition health: {overall_health:.1f}% ({health_status})")
    
    return partition_health