"""
Schedules and sensors for serverless Dagster+ deployment
"""

from datetime import datetime, timedelta
from typing import Optional, List

import dagster as dg
from dagster import (
    schedule, sensor, job, op, RunRequest, RunConfig,
    SkipReason, SensorEvaluationContext, SensorResult,
    DailyPartitionsDefinition, build_schedule_from_partitioned_job,
    DefaultScheduleStatus, DefaultSensorStatus, Config, OpExecutionContext
)

from .serverless_assets import (
    raw_customers, raw_orders, daily_sales, 
    daily_partitions, business_units, hourly_partitions
)
from .resources import api_client, slack, database


# ============================================================================
# BASIC SCHEDULES
# ============================================================================

@schedule(
    job_name="daily_data_refresh",
    cron_schedule="0 2 * * *",  # 2 AM daily
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily refresh of customer and order data"
)
def daily_data_refresh_schedule(context):
    """Schedule daily data refresh at 2 AM"""
    return RunConfig(
        tags={
            "schedule": "daily_data_refresh",
            "environment": "production",
            "scheduled_time": context.scheduled_execution_time.isoformat()
        }
    )


@schedule(
    job_name="business_reporting",
    cron_schedule="0 8 1 * *",  # 8 AM on first day of month
    default_status=DefaultScheduleStatus.RUNNING,
    description="Monthly business reports generation"
)
def monthly_reporting_schedule(context):
    """Generate monthly business reports"""
    return RunConfig(
        tags={
            "schedule": "monthly_reporting",
            "report_type": "monthly",
            "month": context.scheduled_execution_time.strftime("%Y-%m")
        }
    )


# Partitioned asset schedule
daily_sales_schedule = build_schedule_from_partitioned_job(
    job=dg.define_asset_job(
        "scheduled_daily_sales_job",
        selection=[daily_sales],
        partitions_def=daily_partitions
    ),
    name="daily_sales_schedule", 
    description="Process daily sales data for each partition"
)


# ============================================================================
# BASIC SENSORS
# ============================================================================

@sensor(
    job_name="data_quality_check",
    default_status=DefaultSensorStatus.RUNNING,
    description="Monitor data quality metrics and trigger alerts"
)
def data_quality_sensor(context: SensorEvaluationContext):
    """Sensor to check data quality and trigger alerts"""
    
    # Simulate checking external system for data quality issues
    # In real implementation, would check metrics from monitoring system
    current_time = datetime.now()
    last_check = context.cursor or "2024-01-01T00:00:00"
    last_check_time = datetime.fromisoformat(last_check.replace('Z', '+00:00').replace('+00:00', ''))
    
    # Only check every 30 minutes to avoid spam
    if (current_time - last_check_time).total_seconds() < 1800:
        return SkipReason("Waiting for next check interval (30 minutes)")
    
    # Simulate data quality check
    quality_score = 0.85  # Mock quality score
    threshold = 0.90
    
    if quality_score < threshold:
        return SensorResult(
            run_requests=[RunRequest(
                run_key=f"data_quality_{current_time.isoformat()}",
                tags={
                    "sensor": "data_quality_sensor",
                    "quality_score": str(quality_score),
                    "threshold": str(threshold),
                    "alert_type": "data_quality_degradation"
                }
            )],
            cursor=current_time.isoformat()
        )
    else:
        return SensorResult(
            run_requests=[],
            cursor=current_time.isoformat(),
            skip_reason=f"Data quality good: {quality_score:.2%} (threshold: {threshold:.2%})"
        )


@sensor(
    job_name="file_processing",
    default_status=DefaultSensorStatus.RUNNING,
    description="Process new files when they arrive in S3",
    minimum_interval_seconds=60
)
def file_arrival_sensor(context: SensorEvaluationContext):
    """Sensor to detect new files and trigger processing"""
    
    # Simulate checking for new files in S3
    # In real implementation, would use boto3 to list objects
    current_time = datetime.now()
    last_check = context.cursor or "2024-01-01T00:00:00"
    
    # Mock file detection
    new_files = [
        f"data/incoming/customers_{current_time.strftime('%Y%m%d_%H%M%S')}.csv",
        f"data/incoming/orders_{current_time.strftime('%Y%m%d_%H%M%S')}.json"
    ]
    
    if new_files:
        run_requests = []
        for file_path in new_files:
            run_requests.append(RunRequest(
                run_key=f"process_file_{file_path.replace('/', '_')}",
                tags={
                    "sensor": "file_arrival_sensor",
                    "file_path": file_path,
                    "detected_at": current_time.isoformat()
                }
            ))
        
        return SensorResult(
            run_requests=run_requests,
            cursor=current_time.isoformat()
        )
    else:
        return SkipReason("No new files detected")


# ============================================================================
# ADVANCED SENSORS
# ============================================================================

@sensor(
    job_name="cross_partition_reconciliation",
    default_status=DefaultSensorStatus.RUNNING,
    description="Reconcile data across partitions when thresholds are met"
)
def partition_reconciliation_sensor(context: SensorEvaluationContext):
    """Advanced sensor for cross-partition data reconciliation"""
    
    # Simulate checking partition completion status
    current_time = datetime.now()
    
    # Mock partition status check
    completed_partitions = [
        (current_time - timedelta(days=1)).strftime("%Y-%m-%d"),
        (current_time - timedelta(days=2)).strftime("%Y-%m-%d"),
        (current_time - timedelta(days=3)).strftime("%Y-%m-%d")
    ]
    
    # Trigger reconciliation when we have 3+ completed partitions
    if len(completed_partitions) >= 3:
        return RunRequest(
            run_key=f"reconciliation_{current_time.strftime('%Y%m%d_%H%M%S')}",
            tags={
                "sensor": "partition_reconciliation_sensor", 
                "completed_partitions": str(len(completed_partitions)),
                "partition_list": ",".join(completed_partitions),
                "reconciliation_type": "cross_partition"
            }
        )
    else:
        return SkipReason(f"Not enough completed partitions: {len(completed_partitions)} (need 3+)")


@sensor(
    job_name="business_hours_processing",
    default_status=DefaultSensorStatus.STOPPED,  # Only run during business hours
    description="Process high-priority items during business hours only"
)
def business_hours_sensor(context: SensorEvaluationContext):
    """Sensor that only triggers during business hours (9 AM - 5 PM EST)"""
    
    current_time = datetime.now()
    hour = current_time.hour
    weekday = current_time.weekday()  # 0 = Monday, 6 = Sunday
    
    # Business hours: 9 AM - 5 PM, Monday - Friday
    if weekday >= 5 or hour < 9 or hour >= 17:
        return SkipReason(f"Outside business hours: {current_time.strftime('%A %I:%M %p')}")
    
    # Check for high-priority items (simulated)
    high_priority_items = 3  # Mock count
    
    if high_priority_items > 0:
        return RunRequest(
            run_key=f"business_hours_{current_time.strftime('%Y%m%d_%H%M%S')}",
            tags={
                "sensor": "business_hours_sensor",
                "priority": "high",
                "item_count": str(high_priority_items),
                "business_hour": str(hour)
            }
        )
    else:
        return SkipReason("No high-priority items to process")


# ============================================================================
# MULTI-ASSET SENSORS
# ============================================================================

@sensor(
    job_name="analytics_refresh",
    default_status=DefaultSensorStatus.RUNNING,
    description="Refresh analytics when upstream data is updated"
)
def analytics_refresh_sensor(context: SensorEvaluationContext):
    """Sensor to refresh analytics when multiple upstream assets are updated"""
    
    # In real implementation, would check asset materialization events
    # This simulates checking if raw_customers and raw_orders were recently updated
    
    current_time = datetime.now()
    last_check = context.cursor or "2024-01-01T00:00:00"
    last_check_time = datetime.fromisoformat(last_check.replace('Z', '+00:00').replace('+00:00', ''))
    
    # Simulate checking for recent materializations
    # Would use context.instance.get_event_records() in real implementation
    hours_since_check = (current_time - last_check_time).total_seconds() / 3600
    
    if hours_since_check >= 4:  # Check every 4 hours
        # Simulate finding recent asset updates
        return RunRequest(
            run_key=f"analytics_refresh_{current_time.strftime('%Y%m%d_%H%M%S')}",
            tags={
                "sensor": "analytics_refresh_sensor",
                "trigger_reason": "upstream_assets_updated",
                "hours_since_check": str(round(hours_since_check, 2))
            }
        )
    else:
        return SensorResult(
            run_requests=[],
            cursor=current_time.isoformat(),
            skip_reason=f"Too soon since last check: {hours_since_check:.1f}h (need 4h+)"
        )


# ============================================================================
# JOBS FOR SCHEDULES AND SENSORS
# ============================================================================

@op(
    config_schema={"message": str, "priority": str},
    description="Send notification with configurable message and priority"
)
def send_notification(context: OpExecutionContext):
    """Send notification op for various workflows"""
    # Provide defaults if config not available
    config = getattr(context, 'op_config', {})
    message = config.get("message", "Demo notification message")
    priority = config.get("priority", "info")
    
    context.log.info(f"📢 Sending {priority} priority notification: {message}")
    
    # Simulate notification logic
    return {
        "sent_at": datetime.now().isoformat(),
        "message": message,
        "priority": priority,
        "status": "sent"
    }


@op(description="Check data quality metrics and generate report")
def check_data_quality(context: OpExecutionContext):
    """Data quality check operation"""
    
    # Simulate data quality checks
    metrics = {
        "completeness": 0.95,
        "accuracy": 0.87,
        "timeliness": 0.92,
        "consistency": 0.88
    }
    
    overall_score = sum(metrics.values()) / len(metrics)
    
    context.log.info(f"📊 Data quality metrics: {metrics}")
    context.log.info(f"📈 Overall quality score: {overall_score:.2%}")
    
    return {
        "metrics": metrics,
        "overall_score": overall_score,
        "checked_at": datetime.now().isoformat()
    }


@job(
    name="daily_data_refresh", 
    description="Daily refresh of customer and order data",
    config=RunConfig(
        ops={
            "send_notification": {
                "config": {
                    "message": "Daily data refresh completed",
                    "priority": "info"
                }
            }
        }
    )
)
def daily_data_refresh_job():
    """Job to refresh daily data"""
    check_data_quality()
    send_notification()


@job(
    name="business_reporting",
    description="Generate business reports",
    config=RunConfig(
        ops={
            "send_notification": {
                "config": {
                    "message": "Business reports generated successfully",
                    "priority": "info"
                }
            }
        }
    )
)
def business_reporting_job():
    """Job for business reporting"""
    send_notification()


@job(
    name="data_quality_check", 
    description="Check data quality and send alerts if needed",
    config=RunConfig(
        ops={
            "send_notification": {
                "config": {
                    "message": "Data quality check completed",
                    "priority": "warning"
                }
            }
        }
    )
)
def data_quality_check_job():
    """Job triggered by data quality sensor"""
    check_data_quality()
    send_notification()


@job(
    name="file_processing",
    description="Process newly arrived files",
    config=RunConfig(
        ops={
            "send_notification": {
                "config": {
                    "message": "File processing completed",
                    "priority": "info"
                }
            }
        }
    )
)
def file_processing_job():
    """Job triggered by file arrival sensor"""
    send_notification()


@job(
    name="cross_partition_reconciliation",
    description="Reconcile data across completed partitions", 
    config=RunConfig(
        ops={
            "send_notification": {
                "config": {
                    "message": "Partition reconciliation completed",
                    "priority": "info"
                }
            }
        }
    )
)
def cross_partition_reconciliation_job():
    """Job for partition reconciliation"""
    check_data_quality()
    send_notification()


@job(
    name="business_hours_processing",
    description="Process high-priority items during business hours",
    config=RunConfig(
        ops={
            "send_notification": {
                "config": {
                    "message": "Business hours processing completed",
                    "priority": "high"
                }
            }
        }
    )
)
def business_hours_processing_job():
    """Job for business hours processing"""
    send_notification()


@job(
    name="analytics_refresh",
    description="Refresh analytics dashboards and reports",
    config=RunConfig(
        ops={
            "send_notification": {
                "config": {
                    "message": "Analytics refresh completed",
                    "priority": "info"
                }
            }
        }
    )
)
def analytics_refresh_job():
    """Job to refresh analytics"""
    check_data_quality()
    send_notification()


# ============================================================================
# CONFIGURATION-BASED SCHEDULES AND SENSORS
# ============================================================================

class AlertConfig(Config):
    """Configuration for alert thresholds"""
    error_threshold: int = 10
    warning_threshold: int = 5
    notification_channels: List[str] = ["#data-alerts", "#engineering"]


@sensor(
    job_name="configurable_alert_job",
    default_status=DefaultSensorStatus.RUNNING,
    description="Configurable sensor for various alert conditions"
)
def configurable_alert_sensor(context: SensorEvaluationContext, config: AlertConfig):
    """Configurable sensor for different alert conditions"""
    
    # Simulate checking various metrics
    current_errors = 8  # Mock error count
    current_warnings = 12  # Mock warning count
    
    run_requests = []
    
    if current_errors >= config.error_threshold:
        run_requests.append(RunRequest(
            run_key=f"error_alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            tags={
                "alert_type": "error",
                "count": str(current_errors),
                "threshold": str(config.error_threshold),
                "channels": ",".join(config.notification_channels)
            }
        ))
    
    if current_warnings >= config.warning_threshold:
        run_requests.append(RunRequest(
            run_key=f"warning_alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            tags={
                "alert_type": "warning", 
                "count": str(current_warnings),
                "threshold": str(config.warning_threshold),
                "channels": ",".join(config.notification_channels)
            }
        ))
    
    if run_requests:
        return SensorResult(run_requests=run_requests)
    else:
        return SkipReason(f"No alerts needed (errors: {current_errors}, warnings: {current_warnings})")


@job(
    name="configurable_alert_job",
    description="Handle configurable alerts",
    config=RunConfig(
        ops={
            "send_notification": {
                "config": {
                    "message": "Alert handled successfully",
                    "priority": "high"
                }
            }
        }
    )
)
def configurable_alert_job():
    """Job for configurable alerts"""
    send_notification()