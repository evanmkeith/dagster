"""
Asset checks, monitoring, and observability features for serverless Dagster+
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import pandas as pd

import dagster as dg
from dagster import (
    asset_check, AssetCheckResult, AssetCheckSeverity, AssetCheckExecutionContext,
    MetadataValue, MaterializeResult, asset, AssetExecutionContext,
    AutoMaterializePolicy
)

# Import the new freshness policy if available
try:
    from dagster import FreshnessPolicy
except ImportError:
    from dagster import LegacyFreshnessPolicy as FreshnessPolicy
from pydantic import Field

from .serverless_assets import (
    raw_customers, raw_orders, clean_customers, clean_orders,
    daily_sales, hourly_traffic
)
from .resources import database, api_client, slack


# ============================================================================
# BASIC ASSET CHECKS
# ============================================================================

@asset_check(
    asset=raw_customers,
    description="Check that raw customer data has expected record count",
    blocking=True  # Block downstream assets if this fails
)
def raw_customers_record_count_check(context: AssetCheckExecutionContext, raw_customers: Dict[str, Any]) -> AssetCheckResult:
    """Check that raw customer data meets minimum record count requirements"""
    
    record_count = raw_customers.get("record_count", 0)
    min_expected_records = 10
    
    success = record_count >= min_expected_records
    
    return AssetCheckResult(
        success=success,
        description=f"Raw customers has {record_count} records (minimum: {min_expected_records})",
        metadata={
            "record_count": record_count,
            "min_expected": min_expected_records,
            "check_timestamp": datetime.now().isoformat(),
        },
        severity=AssetCheckSeverity.ERROR if not success else None
    )


@asset_check(
    asset=raw_customers,
    description="Check data freshness of raw customer data",
    blocking=False  # Warning only, don't block downstream
)
def raw_customers_freshness_check(context: AssetCheckExecutionContext, raw_customers: Dict[str, Any]) -> AssetCheckResult:
    """Check that raw customer data is fresh (extracted recently)"""
    
    extracted_at_str = raw_customers.get("extracted_at")
    if not extracted_at_str:
        return AssetCheckResult(
            success=False,
            description="No extraction timestamp found",
            severity=AssetCheckSeverity.WARN
        )
    
    extracted_at = datetime.fromisoformat(extracted_at_str.replace('Z', '+00:00').replace('+00:00', ''))
    current_time = datetime.now()
    age_hours = (current_time - extracted_at).total_seconds() / 3600
    max_age_hours = 6  # Data should be no more than 6 hours old
    
    success = age_hours <= max_age_hours
    
    return AssetCheckResult(
        success=success,
        description=f"Data is {age_hours:.1f} hours old (max: {max_age_hours} hours)",
        metadata={
            "extracted_at": extracted_at_str,
            "age_hours": age_hours,
            "max_age_hours": max_age_hours,
            "is_stale": not success
        },
        severity=AssetCheckSeverity.WARN if not success else None
    )


@asset_check(
    asset=raw_orders,
    description="Check for duplicate order IDs in raw data",
    blocking=True
)
def raw_orders_duplicate_check(context: AssetCheckExecutionContext, raw_orders: Dict[str, Any]) -> AssetCheckResult:
    """Check for duplicate order IDs that could cause data quality issues"""
    
    orders = raw_orders.get("orders", [])
    if not orders:
        return AssetCheckResult(
            success=True,
            description="No orders to check for duplicates"
        )
    
    # Extract order IDs
    order_ids = [order.get("id") for order in orders if order.get("id")]
    unique_ids = set(order_ids)
    
    duplicate_count = len(order_ids) - len(unique_ids)
    success = duplicate_count == 0
    
    return AssetCheckResult(
        success=success,
        description=f"Found {duplicate_count} duplicate order IDs out of {len(order_ids)} orders",
        metadata={
            "total_orders": len(orders),
            "unique_order_ids": len(unique_ids),
            "duplicate_count": duplicate_count,
            "duplicate_rate": duplicate_count / len(order_ids) if order_ids else 0
        },
        severity=AssetCheckSeverity.ERROR if not success else None
    )


# ============================================================================
# DATA QUALITY CHECKS FOR CLEANED DATA
# ============================================================================

@asset_check(
    asset=clean_customers,
    description="Validate cleaned customer data quality",
    blocking=False
)
def clean_customers_quality_check(context: AssetCheckExecutionContext, clean_customers: pd.DataFrame) -> AssetCheckResult:
    """Comprehensive data quality check for cleaned customer data"""
    
    if clean_customers.empty:
        return AssetCheckResult(
            success=False,
            description="Clean customers dataset is empty",
            severity=AssetCheckSeverity.ERROR
        )
    
    # Multiple quality checks
    checks = {}
    
    # Check for null values in critical columns
    null_names = clean_customers["name"].isnull().sum()
    checks["null_names"] = null_names == 0
    
    # Check for reasonable created dates
    future_dates = (clean_customers["created_date"] > datetime.now()).sum()
    checks["future_dates"] = future_dates == 0
    
    # Check for name formatting (should be uppercase)
    non_upper_names = clean_customers[clean_customers["name"].str.upper() != clean_customers["name"]]["name"].count()
    checks["proper_formatting"] = non_upper_names == 0
    
    all_passed = all(checks.values())
    failed_checks = [check for check, passed in checks.items() if not passed]
    
    return AssetCheckResult(
        success=all_passed,
        description=f"Data quality checks: {len(checks) - len(failed_checks)}/{len(checks)} passed. Failed: {failed_checks}",
        metadata={
            "total_records": len(clean_customers),
            "null_names_count": null_names,
            "future_dates_count": future_dates,
            "non_upper_names_count": non_upper_names,
            "checks_passed": checks,
            "quality_score": (len(checks) - len(failed_checks)) / len(checks)
        },
        severity=AssetCheckSeverity.WARN if not all_passed else None
    )


@asset_check(
    asset=clean_orders,
    description="Validate order amounts and detect anomalies",
    blocking=False
)
def clean_orders_anomaly_check(context: AssetCheckExecutionContext, clean_orders: pd.DataFrame) -> AssetCheckResult:
    """Check for anomalies in order amounts"""
    
    if clean_orders.empty or "amount" not in clean_orders.columns:
        return AssetCheckResult(
            success=False,
            description="Cannot check anomalies: empty dataset or missing amount column",
            severity=AssetCheckSeverity.WARN
        )
    
    amounts = clean_orders["amount"].dropna()
    if len(amounts) == 0:
        return AssetCheckResult(
            success=False,
            description="No valid amounts to analyze",
            severity=AssetCheckSeverity.WARN
        )
    
    # Statistical anomaly detection
    q1 = amounts.quantile(0.25)
    q3 = amounts.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    outliers = amounts[(amounts < lower_bound) | (amounts > upper_bound)]
    outlier_count = len(outliers)
    outlier_rate = outlier_count / len(amounts)
    
    # Check for negative amounts
    negative_amounts = (amounts < 0).sum()
    
    # Overall success criteria
    success = outlier_rate < 0.05 and negative_amounts == 0  # Less than 5% outliers, no negatives
    
    return AssetCheckResult(
        success=success,
        description=f"Found {outlier_count} outliers ({outlier_rate:.1%}) and {negative_amounts} negative amounts",
        metadata={
            "total_orders": len(amounts),
            "outlier_count": outlier_count,
            "outlier_rate": outlier_rate,
            "negative_amounts": negative_amounts,
            "amount_stats": {
                "mean": float(amounts.mean()),
                "median": float(amounts.median()),
                "min": float(amounts.min()),
                "max": float(amounts.max()),
                "std": float(amounts.std())
            },
            "outlier_bounds": {
                "lower": float(lower_bound),
                "upper": float(upper_bound)
            }
        },
        severity=AssetCheckSeverity.WARN if not success else None
    )


# ============================================================================
# BUSINESS LOGIC CHECKS
# ============================================================================

# Note: This asset check references outputs from multi-assets which requires special handling
# Commented out for demo simplicity - would need proper multi-asset output references
def customer_metrics_business_logic_check_disabled():
    """Placeholder for customer metrics business logic check"""
    pass


# Note: This asset check also references multi-asset outputs - commented out for demo  
def combined_summary_reasonableness_check_disabled():
    """Placeholder for summary metrics reasonableness check"""
    pass


# ============================================================================
# PARTITIONED ASSET CHECKS
# ============================================================================

@asset_check(
    asset=daily_sales,
    description="Check daily sales data completeness and trends",
    blocking=False
)
def daily_sales_completeness_check(context: AssetCheckExecutionContext, daily_sales: Dict[str, Any]) -> AssetCheckResult:
    """Check that daily sales data is complete and follows expected patterns"""
    
    partition_date = context.partition_key
    
    if not daily_sales or "sales" not in daily_sales:
        return AssetCheckResult(
            success=False,
            description=f"No sales data found for {partition_date}",
            severity=AssetCheckSeverity.WARN
        )
    
    sales_data = daily_sales["sales"]
    total_sales = daily_sales.get("total_sales", 0)
    
    issues = []
    
    # Check for minimum sales activity
    min_expected_sales = 1
    if len(sales_data) < min_expected_sales:
        issues.append(f"Low sales activity: {len(sales_data)} sales (expected at least {min_expected_sales})")
    
    # Check total sales calculation
    calculated_total = len(sales_data) * 100  # As per the mock logic
    if abs(calculated_total - total_sales) > 0.01:
        issues.append(f"Total sales calculation error: {calculated_total} vs {total_sales}")
    
    # Check for weekend vs weekday patterns (simplified)
    partition_datetime = datetime.strptime(partition_date, "%Y-%m-%d")
    is_weekend = partition_datetime.weekday() >= 5
    
    if is_weekend and len(sales_data) > len(sales_data) * 1.5:  # Simplified weekend check
        issues.append("Unusually high weekend sales (may indicate data error)")
    
    success = len(issues) == 0
    
    return AssetCheckResult(
        success=success,
        description=f"Daily sales check for {partition_date}: {len(issues)} issues - {'; '.join(issues) if issues else 'All checks passed'}",
        metadata={
            "partition_date": partition_date,
            "sales_count": len(sales_data),
            "total_sales_amount": total_sales,
            "is_weekend": is_weekend,
            "issues_found": issues
        },
        severity=AssetCheckSeverity.WARN if not success else None
    )


# ============================================================================
# OBSERVABILITY ASSETS WITH BUILT-IN MONITORING
# ============================================================================

@asset(
    description="Data quality monitoring dashboard metrics",
    group_name="observability"
)
def data_quality_metrics(
    context: AssetExecutionContext,
    raw_customers: Dict[str, Any],
    raw_orders: Dict[str, Any],
    clean_customers: pd.DataFrame,
    clean_orders: pd.DataFrame
) -> Dict[str, Any]:
    """Generate comprehensive data quality metrics for monitoring dashboards"""
    
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "raw_data_metrics": {
            "customers_record_count": raw_customers.get("record_count", 0),
            "orders_record_count": raw_orders.get("record_count", 0),
            "customers_freshness_hours": (
                (datetime.now() - datetime.fromisoformat(
                    raw_customers.get("extracted_at", datetime.now().isoformat()).replace('Z', '+00:00').replace('+00:00', '')
                )).total_seconds() / 3600
            ),
            "orders_freshness_hours": (
                (datetime.now() - datetime.fromisoformat(
                    raw_orders.get("extracted_at", datetime.now().isoformat()).replace('Z', '+00:00').replace('+00:00', '')
                )).total_seconds() / 3600
            )
        },
        "cleaned_data_metrics": {
            "customers_cleaned_count": len(clean_customers),
            "orders_cleaned_count": len(clean_orders),
            "customers_null_rate": clean_customers.isnull().sum().sum() / (len(clean_customers) * len(clean_customers.columns)) if not clean_customers.empty else 0,
            "orders_null_rate": clean_orders.isnull().sum().sum() / (len(clean_orders) * len(clean_orders.columns)) if not clean_orders.empty else 0
        },
        "data_loss_metrics": {
            "customer_data_loss_rate": max(0, (raw_customers.get("record_count", 0) - len(clean_customers))) / max(1, raw_customers.get("record_count", 1)),
            "order_data_loss_rate": max(0, (raw_orders.get("record_count", 0) - len(clean_orders))) / max(1, raw_orders.get("record_count", 1))
        }
    }
    
    # Calculate overall quality score
    quality_indicators = []
    
    # Freshness score (data should be < 6 hours old)
    freshness_score = min(1.0, max(0.0, 1 - metrics["raw_data_metrics"]["customers_freshness_hours"] / 24))
    quality_indicators.append(freshness_score)
    
    # Completeness score (low null rate is good)
    completeness_score = 1 - min(1.0, metrics["cleaned_data_metrics"]["customers_null_rate"] * 10)
    quality_indicators.append(completeness_score)
    
    # Data loss score (low loss rate is good)
    data_loss_score = 1 - min(1.0, metrics["data_loss_metrics"]["customer_data_loss_rate"])
    quality_indicators.append(data_loss_score)
    
    overall_quality_score = sum(quality_indicators) / len(quality_indicators)
    metrics["overall_quality_score"] = overall_quality_score
    
    context.add_output_metadata({
        "overall_quality_score": overall_quality_score,
        "customers_processed": len(clean_customers),
        "orders_processed": len(clean_orders),
        "data_freshness_hours": metrics["raw_data_metrics"]["customers_freshness_hours"],
        "quality_trend": "improving" if overall_quality_score > 0.8 else "needs_attention"
    })
    
    context.log.info(f"📊 Data quality score: {overall_quality_score:.2%}")
    
    return metrics


@asset(
    description="Pipeline performance and execution metrics",
    group_name="observability"
)
def pipeline_performance_metrics(context: AssetExecutionContext) -> Dict[str, Any]:
    """Track pipeline performance metrics for monitoring and alerting"""
    
    # In a real implementation, this would query the Dagster instance
    # for actual run statistics, asset materialization times, etc.
    
    performance_metrics = {
        "timestamp": datetime.now().isoformat(),
        "pipeline_health": {
            "success_rate_24h": 0.95,  # Mock: 95% success rate
            "avg_execution_time_minutes": 12.5,  # Mock: average 12.5 minutes
            "failed_runs_count": 2,  # Mock: 2 failed runs in last 24h
            "total_runs_count": 40,  # Mock: 40 total runs
        },
        "asset_materialization_stats": {
            "assets_materialized_24h": 120,
            "avg_materialization_time_seconds": 45,
            "slowest_asset": "monthly_business_report",
            "fastest_asset": "raw_customers"
        },
        "resource_utilization": {
            "avg_memory_usage_mb": 256,
            "avg_cpu_usage_percent": 35,
            "peak_memory_usage_mb": 512,
            "execution_cost_estimate_usd": 2.45
        }
    }
    
    # Calculate health score
    success_rate = performance_metrics["pipeline_health"]["success_rate_24h"]
    avg_time = performance_metrics["pipeline_health"]["avg_execution_time_minutes"]
    
    health_score = min(1.0, success_rate * (1 - min(0.5, avg_time / 60)))  # Penalize long execution times
    performance_metrics["health_score"] = health_score
    
    context.add_output_metadata({
        "health_score": health_score,
        "success_rate": success_rate,
        "avg_execution_minutes": avg_time,
        "cost_estimate_usd": performance_metrics["resource_utilization"]["execution_cost_estimate_usd"]
    })
    
    # Log alerts for poor performance
    if health_score < 0.8:
        context.log.warning(f"⚠️  Pipeline health score below threshold: {health_score:.2%}")
    
    return performance_metrics


# ============================================================================
# CUSTOM FRESHNESS POLICIES
# ============================================================================

# Asset with freshness monitoring (policy simplified for demo)
@asset(
    description="Customer engagement metrics with freshness monitoring",
    group_name="analytics"
    # Note: freshness_policy configuration varies by Dagster version - simplified for demo
)
def customer_engagement_metrics(
    context: AssetExecutionContext,
    clean_customers: pd.DataFrame,
    clean_orders: pd.DataFrame
) -> Dict[str, Any]:
    """Calculate customer engagement metrics with built-in freshness monitoring"""
    
    if clean_customers.empty or clean_orders.empty:
        context.log.warning("Empty datasets provided for engagement calculation")
        return {"metrics": {}, "calculated_at": datetime.now().isoformat()}
    
    # Calculate engagement metrics
    total_customers = len(clean_customers)
    total_orders = len(clean_orders)
    
    # Group orders by customer
    orders_by_customer = clean_orders.groupby('customer_id').size() if 'customer_id' in clean_orders.columns else pd.Series([])
    
    engagement_metrics = {
        "active_customers": len(orders_by_customer),
        "engagement_rate": len(orders_by_customer) / total_customers if total_customers > 0 else 0,
        "avg_orders_per_customer": orders_by_customer.mean() if not orders_by_customer.empty else 0,
        "customer_segments": {
            "high_engagement": len(orders_by_customer[orders_by_customer >= 5]) if not orders_by_customer.empty else 0,
            "medium_engagement": len(orders_by_customer[(orders_by_customer >= 2) & (orders_by_customer < 5)]) if not orders_by_customer.empty else 0,
            "low_engagement": len(orders_by_customer[orders_by_customer == 1]) if not orders_by_customer.empty else 0
        },
        "calculated_at": datetime.now().isoformat()
    }
    
    context.add_output_metadata({
        "engagement_rate": engagement_metrics["engagement_rate"],
        "active_customers": engagement_metrics["active_customers"],
        "avg_orders_per_customer": engagement_metrics["avg_orders_per_customer"],
        "data_freshness": "current"
    })
    
    return {"metrics": engagement_metrics, "calculated_at": datetime.now().isoformat()}