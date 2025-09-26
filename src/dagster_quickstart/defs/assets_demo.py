import pandas as pd
import dagster as dg

@dg.asset
def sample_data():
    """Generate sample data instead of reading from file."""
    # Create sample data programmatically
    data = {
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 35, 45, 28, 52],
        'city': ['New York', 'London', 'Paris', 'Tokyo', 'Sydney']
    }
    df = pd.DataFrame(data)
    return df

@dg.asset(deps=[sample_data])
def processed_data(sample_data):
    """Process sample data by adding age groups."""
    df = sample_data.copy()
    
    # Add an age_group column based on the value of age
    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )
    
    return df

@dg.asset(deps=[processed_data])
def data_summary(processed_data):
    """Create a summary of the processed data."""
    summary = {
        'total_records': len(processed_data),
        'age_group_counts': processed_data['age_group'].value_counts().to_dict(),
        'avg_age': processed_data['age'].mean()
    }
    return summary

# Asset Checks
@dg.asset_check(asset=sample_data)
def check_sample_data_completeness(context, sample_data):
    """Check that sample_data has no missing values and expected number of records."""
    
    # Check for missing values
    missing_values = sample_data.isnull().sum().sum()
    
    # Check record count
    expected_records = 5
    actual_records = len(sample_data)
    
    # Check required columns
    required_columns = {'name', 'age', 'city'}
    actual_columns = set(sample_data.columns)
    
    passed = (
        missing_values == 0 and 
        actual_records == expected_records and
        required_columns.issubset(actual_columns)
    )
    
    metadata = {
        "missing_values": missing_values,
        "record_count": actual_records,
        "columns": list(actual_columns)
    }
    
    return dg.AssetCheckResult(
        passed=passed,
        description=f"Sample data completeness check. Missing values: {missing_values}, Records: {actual_records}",
        metadata=metadata
    )

@dg.asset_check(asset=processed_data)
def check_age_groups_valid(context, processed_data):
    """Check that all age groups are correctly assigned and no nulls exist."""
    
    # Check for null age groups
    null_age_groups = processed_data['age_group'].isnull().sum()
    
    # Check that all age groups are valid
    valid_age_groups = {'Young', 'Middle', 'Senior'}
    actual_age_groups = set(processed_data['age_group'].dropna().astype(str))
    
    # Verify age group logic
    young_ages = processed_data[processed_data['age_group'] == 'Young']['age']
    middle_ages = processed_data[processed_data['age_group'] == 'Middle']['age']
    senior_ages = processed_data[processed_data['age_group'] == 'Senior']['age']
    
    age_logic_correct = (
        all(age <= 30 for age in young_ages) and
        all(30 < age <= 40 for age in middle_ages) and
        all(age > 40 for age in senior_ages)
    )
    
    passed = (
        null_age_groups == 0 and 
        actual_age_groups.issubset(valid_age_groups) and
        age_logic_correct
    )
    
    metadata = {
        "null_age_groups": null_age_groups,
        "unique_age_groups": list(actual_age_groups),
        "age_logic_correct": age_logic_correct
    }
    
    return dg.AssetCheckResult(
        passed=passed,
        description=f"Age group validation. Nulls: {null_age_groups}, Logic correct: {age_logic_correct}",
        metadata=metadata
    )

@dg.asset_check(asset=data_summary)
def check_summary_metrics(context, data_summary):
    """Check that summary metrics are reasonable and complete."""
    
    required_keys = {'total_records', 'age_group_counts', 'avg_age'}
    actual_keys = set(data_summary.keys())
    
    # Check total records is positive
    total_records = data_summary.get('total_records', 0)
    
    # Check average age is reasonable (between 0 and 120)
    avg_age = data_summary.get('avg_age', 0)
    
    # Check age group counts sum to total
    age_group_counts = data_summary.get('age_group_counts', {})
    age_group_sum = sum(age_group_counts.values()) if age_group_counts else 0
    
    passed = (
        required_keys.issubset(actual_keys) and
        total_records > 0 and
        0 <= avg_age <= 120 and
        age_group_sum == total_records
    )
    
    metadata = {
        "total_records": total_records,
        "avg_age": avg_age,
        "age_group_sum": age_group_sum,
        "keys_present": list(actual_keys)
    }
    
    return dg.AssetCheckResult(
        passed=passed,
        description=f"Summary metrics validation. Records: {total_records}, Avg age: {avg_age:.1f}",
        metadata=metadata
    )