import pandas as pd
import dagster as dg
from datetime import datetime, timedelta

# ============================================================================
# Modern Freshness Policy Examples using Schedules and AutoMaterializePolicies
# ============================================================================

@dg.asset(
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    description="Raw sensor data that updates frequently"
)
def sensor_readings():
    """Simulate sensor data that updates frequently."""
    current_time = datetime.now()
    
    # Simulate some sensor readings
    data = {
        'timestamp': [current_time - timedelta(minutes=i) for i in range(10)],
        'temperature': [20 + (i * 2.5) for i in range(10)],
        'humidity': [45 + (i * 3) for i in range(10)],
        'pressure': [1013 + (i * 0.5) for i in range(10)]
    }
    
    df = pd.DataFrame(data)
    return df

@dg.asset(
    deps=[sensor_readings],
    auto_materialize_policy=dg.AutoMaterializePolicy.lazy(),
    legacy_freshness_policy=dg.LegacyFreshnessPolicy(
        maximum_lag_minutes=60,  # Data should be no older than 1 hour
        cron_schedule="0 9 * * *"  # Check freshness daily at 9 AM
    ),
    description="Daily report that should be fresh by 9 AM each day"
)
def daily_sensor_report(sensor_readings):
    """Generate a daily summary report from sensor data."""
    
    # Calculate daily statistics
    summary = {
        'date': datetime.now().date(),
        'avg_temperature': sensor_readings['temperature'].mean(),
        'max_temperature': sensor_readings['temperature'].max(),
        'min_temperature': sensor_readings['temperature'].min(),
        'avg_humidity': sensor_readings['humidity'].mean(),
        'avg_pressure': sensor_readings['pressure'].mean(),
        'reading_count': len(sensor_readings),
        'report_generated_at': datetime.now()
    }
    
    return summary

@dg.asset(
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    legacy_freshness_policy=dg.LegacyFreshnessPolicy(
        maximum_lag_minutes=240,  # 4 hours max lag
        cron_schedule="0 */4 * * *"  # Check every 4 hours
    ),
    description="Business metrics that must be fresh every 4 hours"
)
def business_metrics():
    """Critical business metrics with strict freshness requirements."""
    current_time = datetime.now()
    
    # Simulate business KPIs
    metrics = {
        'timestamp': current_time,
        'daily_revenue': 50000 + (current_time.hour * 1000),
        'active_users': 1200 + (current_time.hour * 50),
        'conversion_rate': 0.05 + (current_time.hour * 0.001),
        'customer_satisfaction': 4.2 + (current_time.minute * 0.01),
    }
    
    return metrics

@dg.asset(
    deps=[daily_sensor_report, business_metrics],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager().with_rules(
        dg.AutoMaterializeRule.materialize_on_required_for_freshness()
    ),
    description="Executive dashboard that requires all upstream data to be fresh"
)
def executive_dashboard(daily_sensor_report, business_metrics):
    """Executive dashboard combining sensor and business data."""
    
    dashboard_data = {
        'generated_at': datetime.now(),
        'sensor_summary': {
            'avg_temp': daily_sensor_report['avg_temperature'],
            'reading_count': daily_sensor_report['reading_count']
        },
        'business_summary': {
            'revenue': business_metrics['daily_revenue'],
            'users': business_metrics['active_users'],
            'conversion': business_metrics['conversion_rate']
        },
        'freshness_score': 'FRESH' if datetime.now() - business_metrics['timestamp'] < timedelta(hours=4) else 'STALE'
    }
    
    return dashboard_data

# ============================================================================
# Freshness-Aware Asset with Custom Logic
# ============================================================================

@dg.asset(
    legacy_freshness_policy=dg.LegacyFreshnessPolicy(
        maximum_lag_minutes=30,  # Should be generated within 30 minutes of 8 AM
        cron_schedule="0 8 * * 1-5"  # Weekdays at 8 AM
    ),
    description="Morning report with built-in freshness validation"
)
def morning_briefing():
    """Morning briefing report with freshness awareness."""
    current_time = datetime.now()
    
    # Check if this is running at the expected time
    expected_hour = 8
    is_on_time = abs(current_time.hour - expected_hour) <= 1
    
    briefing = {
        'timestamp': current_time,
        'is_on_time': is_on_time,
        'message': f"Morning briefing generated at {current_time.strftime('%Y-%m-%d %H:%M:%S')}",
        'timeliness_status': 'ON_TIME' if is_on_time else 'DELAYED',
        'data_freshness': {
            'generated_within_hour': is_on_time,
            'acceptable_delay_minutes': 60,
            'actual_delay_minutes': abs((current_time.hour - expected_hour) * 60)
        }
    }
    
    return briefing

# ============================================================================
# Asset with Time-Window Based Freshness
# ============================================================================

@dg.asset(
    legacy_freshness_policy=dg.LegacyFreshnessPolicy(maximum_lag_minutes=15),  # Always fresh within 15 minutes
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    description="Real-time metrics that should always be very fresh"
)
def realtime_metrics():
    """Real-time business metrics that should be refreshed very frequently."""
    current_time = datetime.now()
    
    # Simulate real-time data
    metrics = {
        'timestamp': current_time,
        'current_active_sessions': 150 + (current_time.minute * 2),
        'requests_per_minute': 800 + (current_time.second * 10),
        'error_rate': max(0.001, 0.05 - (current_time.minute * 0.001)),
        'response_time_ms': 120 + (current_time.second),
        'system_load': min(1.0, current_time.minute / 60 + 0.2)
    }
    
    return metrics

# ============================================================================
# Asset Checks for Freshness Validation
# ============================================================================

@dg.asset_check(asset=business_metrics)
def check_business_metrics_freshness(context, business_metrics):
    """Check that business metrics are not older than 4 hours."""
    
    current_time = datetime.now()
    data_timestamp = business_metrics['timestamp']
    age_hours = (current_time - data_timestamp).total_seconds() / 3600
    
    max_age_hours = 4
    passed = age_hours <= max_age_hours
    
    return dg.AssetCheckResult(
        passed=passed,
        description=f"Business metrics are {age_hours:.1f} hours old (max allowed: {max_age_hours})",
        metadata={
            "data_timestamp": data_timestamp.isoformat(),
            "current_time": current_time.isoformat(),
            "age_hours": age_hours,
            "max_age_hours": max_age_hours,
            "is_fresh": passed
        }
    )

@dg.asset_check(asset=morning_briefing)
def check_morning_briefing_timeliness(context, morning_briefing):
    """Check that morning briefing was generated on time."""
    
    is_on_time = morning_briefing['is_on_time']
    delay_minutes = morning_briefing['data_freshness']['actual_delay_minutes']
    
    return dg.AssetCheckResult(
        passed=is_on_time,
        description=f"Morning briefing timeliness check. Delay: {delay_minutes} minutes",
        metadata={
            "is_on_time": is_on_time,
            "delay_minutes": delay_minutes,
            "timeliness_status": morning_briefing['timeliness_status']
        }
    )

@dg.asset_check(asset=executive_dashboard)
def check_dashboard_data_freshness(context, executive_dashboard):
    """Check that dashboard is using fresh data from all sources."""
    
    freshness_score = executive_dashboard['freshness_score']
    passed = freshness_score == 'FRESH'
    
    return dg.AssetCheckResult(
        passed=passed,
        description=f"Dashboard freshness score: {freshness_score}",
        metadata={
            "freshness_score": freshness_score,
            "dashboard_generated_at": executive_dashboard['generated_at'].isoformat(),
            "all_sources_fresh": passed
        }
    )