"""
Script: clean_obfuscated_rows.py
-------------------------------
Removes rows where all key columns (location, company) are obfuscated.
Input: data/silver/enriched_jobs.csv
Output: data/silver/enriched_jobs.csv
Only use this if separate obfuscation cleaning is needed.
Otherwise, it's already handled in the main ETL pipeline.
"""

# Import necessary libraries
from pathlib import Path
import pandas as pd
from extractors.obfuscation_cleaner import drop_obfuscated_rows

# Define input/output paths
INPUT_PATH = Path("data/silver/enriched_jobs.csv")
OUTPUT_PATH = Path("data/silver/enriched_jobs.csv")

# Check if input file exists
if not INPUT_PATH.exists():
    raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

# Read the input CSV file
df = pd.read_csv(INPUT_PATH)

# Remove rows where all key columns (location, company) are obfuscated
df_clean = drop_obfuscated_rows(df, key_cols=['location', 'company'])

# Ensure output directory exists
# and save the cleaned dataset
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
df_clean.to_csv(OUTPUT_PATH, index=False)
print(f"✅ Cleaned data saved to: {OUTPUT_PATH}")
