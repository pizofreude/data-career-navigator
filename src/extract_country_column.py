"""
Script: extract_country_column.py
-------------------------------
Extracts country from the 'location' column and adds it as a new column.
Input: data/silver/enriched_jobs.csv
Output: data/silver/enriched_jobs.csv
Use this script only if the 'country' column is missing or needs to be updated.
Otherwise, it's already handled in the main ETL pipeline.
"""

# Import necessary libraries
import time
from pathlib import Path
import pandas as pd
from extractors.location_extractor import extract_country

# Define input/output paths
INPUT_PATH = Path("data/silver/enriched_jobs.csv")
OUTPUT_PATH = Path("data/silver/enriched_jobs.csv")

# Set SAMPLE_SIZE to None to process all rows, or to an integer for a sample
SAMPLE_SIZE = None  # e.g., 10 for testing

# Check if input file exists
if not INPUT_PATH.exists():
    raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

# Read the input CSV file
df = pd.read_csv(INPUT_PATH)

# Check if 'location' column exists
# and drop rows where all key columns (location, company) are obfuscated
# if not, add a 'country' column with 'Unknown' value
if not 'location' in df.columns:
    print("No 'location' column found. Adding 'country' column with 'Unknown' value.")
    df['country'] = 'Unknown'
if 'location' in df.columns:
    print(f"Extracting country from location column for {len(df)} rows. This may take a while if geocoding is needed...")
    start = time.time()
    if SAMPLE_SIZE:
        df_sample = df.head(SAMPLE_SIZE).copy()
        df_sample['country'] = df_sample['location'].apply(extract_country)
        print(df_sample[['location', 'country']])
        print(f"Sample extraction complete in {time.time() - start:.2f} seconds.")
        # Optionally, stop here for testing
        exit()
    else:
        df['country'] = df['location'].apply(extract_country)
        print(f"Country extraction complete in {time.time() - start:.2f} seconds.")
else:
    df['country'] = 'Unknown'

# Ensure output directory exists and save the updated DataFrame
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False)
print(f"✅ Data with country column saved to: {OUTPUT_PATH}")
