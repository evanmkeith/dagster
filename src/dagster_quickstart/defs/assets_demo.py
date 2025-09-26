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