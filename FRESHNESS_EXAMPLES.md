# Freshness Policy Examples

This repo now includes comprehensive examples of modern freshness policies in Dagster 1.11+.

## What's Included

### Assets with Freshness Policies (`src/dagster_quickstart/defs/freshness_example.py`)

1. **`sensor_readings`** - Basic auto-materializing asset
   - Uses `AutoMaterializePolicy.eager()`
   - Simulates frequent sensor data updates

2. **`daily_sensor_report`** - Daily report with freshness requirements
   - **Legacy Freshness Policy**: Data must be ≤60 minutes old by 9 AM daily
   - Uses `AutoMaterializePolicy.lazy()`
   - Depends on `sensor_readings`

3. **`business_metrics`** - Business KPIs with 4-hour freshness window
   - **Legacy Freshness Policy**: Data must be ≤240 minutes old, checked every 4 hours
   - Uses `AutoMaterializePolicy.eager()`
   - Critical business metrics simulation

4. **`executive_dashboard`** - Dashboard requiring fresh upstream data
   - Uses `AutoMaterializePolicy.eager().with_rules()`
   - Includes `AutoMaterializeRule.materialize_on_required_for_freshness()`
   - Combines sensor and business data

5. **`morning_briefing`** - Weekday morning report
   - **Legacy Freshness Policy**: Must be generated within 30 minutes of 8 AM on weekdays
   - Includes timeliness validation logic

6. **`realtime_metrics`** - Real-time data requiring constant freshness
   - **Legacy Freshness Policy**: Always fresh within 15 minutes
   - Uses `AutoMaterializePolicy.eager()`
   - Simulates real-time business metrics

### Asset Checks for Freshness Validation

- **`check_business_metrics_freshness`** - Validates business metrics are <4 hours old
- **`check_morning_briefing_timeliness`** - Ensures morning briefing runs on time
- **`check_dashboard_data_freshness`** - Verifies dashboard uses fresh source data

## Key Concepts Demonstrated

### **Legacy Freshness Policies**
```python
legacy_freshness_policy=dg.LegacyFreshnessPolicy(
    maximum_lag_minutes=60,
    cron_schedule="0 9 * * *"
)
```

### **Auto-Materialize Policies**
```python
# Eager - materializes immediately when conditions are met
auto_materialize_policy=dg.AutoMaterializePolicy.eager()

# Lazy - waits for explicit triggers
auto_materialize_policy=dg.AutoMaterializePolicy.lazy()

# With freshness rules
auto_materialize_policy=dg.AutoMaterializePolicy.eager().with_rules(
    dg.AutoMaterializeRule.materialize_on_required_for_freshness()
)
```

### **Asset Checks for Freshness**
```python
@dg.asset_check(asset=my_asset)
def check_freshness(context, my_asset):
    age_hours = calculate_data_age(my_asset)
    return dg.AssetCheckResult(
        passed=age_hours <= 4,
        description=f"Data is {age_hours:.1f} hours old"
    )
```

## How to Use

### **Local Development**
```bash
# Start the UI
dg dev

# View assets in browser at http://localhost:3000
```

### **What You'll See in the UI**

1. **Asset Graph**: Shows freshness policies and dependencies
2. **Asset Details**: Each asset shows its freshness configuration
3. **Health Status**: Displays "No freshness policy defined" vs actual freshness policies
4. **Automation Details**: Shows auto-materialize policies and rules

### **Testing Freshness**

1. **Materialize assets** to see initial state
2. **Wait and observe** freshness status changes over time
3. **Check asset checks** to see freshness validation results
4. **Use filters** to view only assets with freshness policies

## Migration Note

This example uses `LegacyFreshnessPolicy` because:
- `FreshnessPolicy` was renamed to `LegacyFreshnessPolicy` in Dagster 1.11+
- The parameter name is `legacy_freshness_policy` (not `freshness_policy`)
- Modern freshness should use automation conditions, but this shows the legacy approach

For truly modern freshness policies, consider using automation conditions with freshness-aware rules instead of legacy freshness policies.

## Files Added

- `src/dagster_quickstart/defs/freshness_example.py` - Main freshness policy examples
- `FRESHNESS_EXAMPLES.md` - This documentation file

## Next Steps

1. Run `dg dev` to explore the assets
2. Materialize some assets to see freshness in action
3. Experiment with different freshness policy parameters
4. Try creating your own freshness-aware assets