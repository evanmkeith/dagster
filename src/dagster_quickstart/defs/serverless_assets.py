"""
Comprehensive assets showcasing Dagster+ serverless features
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any

import dagster as dg
from dagster import (
    asset, multi_asset, AssetOut, AssetIn, Config, MetadataValue, 
    MaterializeResult, AssetExecutionContext, DailyPartitionsDefinition,
    StaticPartitionsDefinition, HourlyPartitionsDefinition, Output,
    AutomationCondition, AssetKey
)

from .resources import S3IOManager, DatabaseResource, APIClient, SlackResource


# ============================================================================
# BASIC ASSETS
# ============================================================================

@asset(
    group_name="raw_data",
    description="Extract raw customer data from external API",
    automation_condition=AutomationCondition.on_cron("0 2 * * *"),  # 2 AM daily
)
def raw_customers(api_client: APIClient) -> Dict[str, Any]:
    """Extract customer data from external API"""
    data = api_client.fetch_data("customers")
    return {
        "customers": data["data"],
        "extracted_at": datetime.now().isoformat(),
        "record_count": len(data["data"])
    }


@asset(
    group_name="raw_data", 
    description="Extract raw orders data",
    automation_condition=AutomationCondition.on_cron("0 2 * * *"),
)
def raw_orders(api_client: APIClient) -> Dict[str, Any]:
    """Extract orders data from external API"""
    data = api_client.fetch_data("orders")
    return {
        "orders": data["data"],
        "extracted_at": datetime.now().isoformat(),
        "record_count": len(data["data"])
    }


@asset(
    group_name="staging",
    description="Clean and validate customer data",
    deps=[raw_customers],
)
def clean_customers(context: AssetExecutionContext, raw_customers: Dict[str, Any]) -> pd.DataFrame:
    """Clean and validate customer data"""
    context.log.info(f"Processing {raw_customers['record_count']} customer records")
    
    # Simulate data cleaning
    customers = pd.DataFrame(raw_customers["customers"])
    
    # Add some cleaning logic
    customers["name"] = customers.get("name", "").str.upper()
    customers["created_date"] = pd.to_datetime(customers.get("timestamp", datetime.now().isoformat()))
    
    context.add_output_metadata({
        "num_records": len(customers),
        "columns": MetadataValue.json(list(customers.columns)),
        "preview": MetadataValue.md(customers.head().to_markdown()),
    })
    
    return customers


@asset(
    group_name="staging",
    description="Clean and validate orders data", 
    deps=[raw_orders],
)
def clean_orders(context: AssetExecutionContext, raw_orders: Dict[str, Any]) -> pd.DataFrame:
    """Clean and validate orders data"""
    context.log.info(f"Processing {raw_orders['record_count']} order records")
    
    orders = pd.DataFrame(raw_orders["orders"])
    orders["order_date"] = pd.to_datetime(orders.get("timestamp", datetime.now().isoformat()))
    orders["amount"] = pd.to_numeric(orders.get("value", 0), errors='coerce')
    
    context.add_output_metadata({
        "num_records": len(orders),
        "total_amount": float(orders["amount"].sum()),
        "date_range": f"{orders['order_date'].min()} to {orders['order_date'].max()}",
    })
    
    return orders


# ============================================================================
# MULTI-ASSET
# ============================================================================

@multi_asset(
    outs={
        "customer_metrics": AssetOut(
            description="Customer-level metrics and KPIs",
            group_name="analytics"
        ),
        "order_metrics": AssetOut(
            description="Order-level metrics and KPIs", 
            group_name="analytics"
        ),
        "combined_summary": AssetOut(
            description="Combined business summary metrics",
            group_name="analytics"
        )
    },
    description="Calculate multiple analytics metrics in one operation"
)
def analytics_metrics(
    context: AssetExecutionContext, 
    clean_customers: pd.DataFrame, 
    clean_orders: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """Calculate comprehensive analytics metrics"""
    
    # Customer metrics
    customer_metrics = clean_customers.groupby('id').agg({
        'name': 'first',
        'created_date': 'first'
    }).reset_index()
    customer_metrics['days_since_signup'] = (
        datetime.now() - customer_metrics['created_date']
    ).dt.days
    
    # Order metrics  
    order_metrics = clean_orders.groupby('customer_id').agg({
        'amount': ['sum', 'mean', 'count'],
        'order_date': ['min', 'max']
    }).reset_index()
    order_metrics.columns = [
        'customer_id', 'total_amount', 'avg_amount', 'order_count', 'first_order', 'last_order'
    ]
    
    # Combined summary
    combined_summary = {
        "total_customers": len(customer_metrics),
        "total_orders": len(clean_orders),
        "total_revenue": float(clean_orders["amount"].sum()),
        "avg_order_value": float(clean_orders["amount"].mean()),
        "generated_at": datetime.now().isoformat()
    }
    
    context.add_output_metadata({
        "customer_metrics": {"records": len(customer_metrics)},
        "order_metrics": {"records": len(order_metrics)},
        "combined_summary": {"metrics_count": len(combined_summary)}
    })
    
    return customer_metrics, order_metrics, combined_summary


# ============================================================================
# PARTITIONED ASSETS
# ============================================================================

daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

@asset(
    partitions_def=daily_partitions,
    group_name="partitioned",
    description="Daily sales data partitioned by date",
    automation_condition=AutomationCondition.on_cron("0 6 * * *"),
)
def daily_sales(context: AssetExecutionContext, database: DatabaseResource) -> Dict[str, Any]:
    """Generate daily sales data for each partition"""
    partition_date = context.partition_key
    context.log.info(f"Processing sales for {partition_date}")
    
    # Simulate querying sales data for specific date
    sql = f"SELECT * FROM sales WHERE date = '{partition_date}'"
    result = database.query(sql)
    
    # Simulate daily sales data
    daily_data = {
        "date": partition_date,
        "sales": result["rows"],
        "total_sales": len(result["rows"]) * 100,  # Mock calculation
        "processed_at": datetime.now().isoformat()
    }
    
    context.add_output_metadata({
        "date": partition_date,
        "record_count": len(result["rows"]),
        "total_amount": daily_data["total_sales"],
    })
    
    return daily_data


hourly_partitions = HourlyPartitionsDefinition(start_date="2024-01-01-00:00")

@asset(
    partitions_def=hourly_partitions,
    group_name="partitioned", 
    description="Hourly website traffic data",
    automation_condition=AutomationCondition.on_cron("5 * * * *"),  # 5 minutes past each hour
)
def hourly_traffic(context: AssetExecutionContext, api_client: APIClient) -> Dict[str, Any]:
    """Collect hourly traffic data"""
    partition_hour = context.partition_key
    context.log.info(f"Processing traffic for hour: {partition_hour}")
    
    # Simulate API call for traffic data
    traffic_data = api_client.fetch_data(f"traffic?hour={partition_hour}")
    
    return {
        "hour": partition_hour,
        "page_views": len(traffic_data["data"]) * 10,
        "unique_visitors": len(traffic_data["data"]) * 7,
        "bounce_rate": 0.35,
        "processed_at": datetime.now().isoformat()
    }


# Static partitions for different business units
business_units = StaticPartitionsDefinition(["sales", "marketing", "product", "engineering"])

@asset(
    partitions_def=business_units,
    group_name="partitioned",
    description="Department-specific KPIs by business unit",
)
def department_kpis(context: AssetExecutionContext, database: DatabaseResource) -> Dict[str, Any]:
    """Calculate KPIs for each business unit"""
    department = context.partition_key
    context.log.info(f"Calculating KPIs for {department} department")
    
    # Simulate department-specific queries
    sql = f"SELECT * FROM {department}_metrics WHERE active = true"
    result = database.query(sql)
    
    # Mock KPI calculations
    kpis = {
        "department": department,
        "employee_count": len(result["rows"]),
        "budget_utilization": 0.85,
        "performance_score": 4.2,
        "last_updated": datetime.now().isoformat()
    }
    
    context.add_output_metadata({
        "department": department,
        "metrics_calculated": len(kpis),
        "data_quality_score": 0.95,
    })
    
    return kpis


# ============================================================================
# DEPENDENT ASSETS WITH COMPLEX LINEAGE
# ============================================================================

@asset(
    group_name="analytics",
    description="Monthly business report combining multiple data sources",
    deps=[daily_sales, "customer_metrics", "order_metrics"],
)
def monthly_business_report(
    context: AssetExecutionContext,
    customer_metrics: pd.DataFrame,
    order_metrics: pd.DataFrame,
    database: DatabaseResource
) -> Dict[str, Any]:
    """Generate comprehensive monthly business report"""
    
    # Simulate complex business logic
    total_customers = len(customer_metrics)
    total_revenue = order_metrics["total_amount"].sum()
    avg_customer_value = total_revenue / total_customers if total_customers > 0 else 0
    
    report = {
        "report_period": datetime.now().strftime("%Y-%m"),
        "key_metrics": {
            "total_customers": total_customers,
            "total_revenue": float(total_revenue),
            "avg_customer_lifetime_value": float(avg_customer_value),
            "customer_growth_rate": 0.15,  # Mock calculation
            "revenue_growth_rate": 0.12,   # Mock calculation
        },
        "trends": {
            "top_customers": customer_metrics.nlargest(5, 'days_since_signup').to_dict('records'),
            "revenue_trend": "increasing",
            "customer_acquisition_trend": "stable"
        },
        "generated_at": datetime.now().isoformat(),
        "data_sources": ["customers", "orders", "daily_sales"]
    }
    
    context.add_output_metadata({
        "report_size_kb": len(json.dumps(report)) / 1024,
        "data_freshness_hours": 1,
        "quality_score": 0.98,
        "key_metrics_count": len(report["key_metrics"]),
    })
    
    return report


# ============================================================================
# EXTERNAL ASSETS (from other systems)
# ============================================================================

@asset(
    key=AssetKey(["external", "stripe_payments"]),
    group_name="external",
    description="Payment data from Stripe (external system)"
)
def external_stripe_data(api_client: APIClient) -> Dict[str, Any]:
    """Mock external Stripe payment data"""
    return {
        "payments": [
            {"id": "pay_123", "amount": 100.00, "status": "completed"},
            {"id": "pay_456", "amount": 250.50, "status": "completed"}
        ],
        "source": "stripe_api",
        "extracted_at": datetime.now().isoformat()
    }

@asset(
    key=AssetKey(["external", "salesforce_leads"]),
    group_name="external", 
    description="Lead data from Salesforce (external system)"
)
def external_salesforce_data(api_client: APIClient) -> Dict[str, Any]:
    """Mock external Salesforce lead data"""
    return {
        "leads": [
            {"id": "lead_789", "score": 85, "status": "qualified"},
            {"id": "lead_012", "score": 92, "status": "hot"}
        ],
        "source": "salesforce_api",
        "extracted_at": datetime.now().isoformat()
    }

@asset(
    group_name="integration",
    description="Reconcile internal orders with external payment data",
    deps=[clean_orders, external_stripe_data],
)
def payment_reconciliation(
    context: AssetExecutionContext, 
    clean_orders: pd.DataFrame,
    slack: SlackResource
) -> Dict[str, Any]:
    """Reconcile internal orders with external Stripe payments"""
    
    # Simulate reconciliation logic
    total_orders = len(clean_orders)
    total_amount = clean_orders["amount"].sum()
    
    # Mock reconciliation results
    reconciled_count = int(total_orders * 0.95)  # 95% successfully reconciled
    discrepancies = total_orders - reconciled_count
    
    reconciliation_report = {
        "total_orders": total_orders,
        "reconciled_orders": reconciled_count,
        "discrepancies": discrepancies,
        "reconciliation_rate": reconciled_count / total_orders if total_orders > 0 else 0,
        "total_amount_reconciled": float(total_amount * 0.95),
        "processed_at": datetime.now().isoformat()
    }
    
    # Send alert if discrepancies are high
    if discrepancies > 5:
        slack.send_alert(
            "Payment Reconciliation Alert",
            f"Found {discrepancies} discrepancies in payment reconciliation"
        )
    
    context.add_output_metadata({
        "reconciliation_rate": reconciliation_report["reconciliation_rate"],
        "discrepancies_found": discrepancies,
        "alert_sent": discrepancies > 5,
    })
    
    return reconciliation_report


# ============================================================================
# CONFIGURATION-BASED ASSETS
# ============================================================================

class ReportConfig(Config):
    """Configuration for flexible reporting"""
    report_type: str = "summary"
    include_charts: bool = True
    email_recipients: List[str] = ["data-team@company.com"]
    custom_filters: Dict[str, Any] = {}

@asset(
    group_name="reporting",
    description="Configurable business report with various options",
)
def configurable_report(
    context: AssetExecutionContext,
    config: ReportConfig,
    combined_summary: Dict[str, Any],
    slack: SlackResource
) -> Dict[str, Any]:
    """Generate a configurable business report"""
    
    report_data = {
        "config": {
            "report_type": config.report_type,
            "include_charts": config.include_charts,
            "recipients": config.email_recipients,
            "filters_applied": list(config.custom_filters.keys())
        },
        "data": combined_summary,
        "generated_at": datetime.now().isoformat()
    }
    
    # Simulate different report types
    if config.report_type == "detailed":
        report_data["additional_metrics"] = {
            "conversion_rate": 0.045,
            "churn_rate": 0.02,
            "ltv_cac_ratio": 3.2
        }
    
    # Notify recipients
    if config.email_recipients:
        recipients_str = ", ".join(config.email_recipients)
        slack.send_message(f"📊 {config.report_type.title()} report generated for {recipients_str}")
    
    context.add_output_metadata({
        "report_type": config.report_type,
        "recipient_count": len(config.email_recipients),
        "includes_charts": config.include_charts,
        "filters_count": len(config.custom_filters),
    })
    
    return report_data