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
def processed_data():
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