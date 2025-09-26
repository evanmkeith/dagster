import pandas as pd
import dagster as dg
from pathlib import Path
import os

# Get the directory where this file is located
current_dir = Path(__file__).parent

# Use relative paths that work both locally and in serverless
sample_data_file = current_dir / "data" / "sample_data.csv"
processed_data_file = current_dir / "data" / "processed_data.csv"

@dg.asset
def processed_data_test():
    """Process sample data by adding age groups."""
    # Read data from the CSV
    df = pd.read_csv(sample_data_file)

    # Add an age_group column based on the value of age
    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    # Save processed data
    processed_data_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(processed_data_file, index=False)
    
    return f"Data processed successfully. Shape: {df.shape}"

@dg.asset_check(asset=processed_data_test)
def check_processed_data_file_exists(context):
    """Check that the processed CSV file was created successfully."""
    
    # Check if the file exists
    file_exists = processed_data_file.exists()
    
    if file_exists:
        # Check file size
        file_size = processed_data_file.stat().st_size
        
        # Try to read the file to ensure it's valid CSV
        try:
            df = pd.read_csv(processed_data_file)
            has_age_group_column = 'age_group' in df.columns
            record_count = len(df)
            readable = True
        except Exception as e:
            readable = False
            has_age_group_column = False
            record_count = 0
            file_size = 0
    else:
        file_size = 0
        readable = False
        has_age_group_column = False
        record_count = 0
    
    passed = (
        file_exists and 
        file_size > 0 and 
        readable and 
        has_age_group_column and
        record_count > 0
    )
    
    metadata = {
        "file_exists": file_exists,
        "file_size_bytes": file_size,
        "readable": readable,
        "has_age_group_column": has_age_group_column,
        "record_count": record_count,
        "file_path": str(processed_data_file)
    }
    
    return dg.AssetCheckResult(
        passed=passed,
        description=f"Processed CSV file check. Exists: {file_exists}, Records: {record_count}, Size: {file_size} bytes",
        metadata=metadata
    )