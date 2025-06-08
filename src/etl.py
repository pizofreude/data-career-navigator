"""
File: src/etl.py

ETL pipeline to enrich job listings with extracted insights:
- Salary estimates
- Experience level
- Technical skill categories
- Work type and employment type

Input:
    data/bronze/clean_jobs.csv

Output:
    data/silver/enriched_jobs.csv
"""

# Import necessary libraries
import sys
from pathlib import Path
import pandas as pd

# Ensure parent directory is in sys.path for imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Import custom extractors
from extractors.salary_extractor import SalaryETL
from extractors.experience_extractor import categorize_experience
from extractors.skills_extractor import extract_skills
from extractors.job_type_extractor import extract_work_type, extract_employment_type

# Define input/output paths
INPUT_PATH = Path("data/bronze/clean_jobs.csv")
OUTPUT_PATH = Path("data/silver/enriched_jobs.csv")

def enrich_job_postings(df):
    """
    Enriches a DataFrame with additional features extracted from job postings.

    Parameters:
        df (pd.DataFrame): Raw DataFrame with at least 'title' and 'description' columns.

    Returns:
        pd.DataFrame: Enriched DataFrame with new columns.
    """
    # Ensure required columns exist
    required_cols = ['title', 'description']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Input DataFrame is missing required columns: {missing}")

    # --- Normalize text fields before any extraction ---
    df = df.copy()
    df['title'] = df['title'].fillna('').astype(str).str.strip()
    df['description'] = df['description'].fillna('').astype(str).str.strip()

    # Salary Extraction using SalaryETL
    salary_etl = SalaryETL()
    df = salary_etl.process_job_dataframe(df, text_column='description', include_title=True, title_column='title')

    # Experience Level Extraction
    df['experience_level'] = df.apply(
        lambda row: categorize_experience(str(row.get('title', '')), str(row.get('description', ''))),
        axis=1
    )

    # Skill Extraction
    skill_columns = ['programming_languages', 'libraries', 'analyst_tools', 'cloud_platforms']
    extracted_skills = df['description'].apply(extract_skills)
    for col in skill_columns:
        df[col] = extracted_skills.apply(lambda skills: skills.get(col, []))

    # Work Type and Employment Type Extraction
    df['work_type'] = df.apply(
        lambda row: extract_work_type(str(row.get('title', '')), str(row.get('description', ''))),
        axis=1
    )
    df['employment_type'] = df.apply(
        lambda row: extract_employment_type(str(row.get('title', '')), str(row.get('description', ''))),
        axis=1
    )

    return df

def main():
    """Main ETL routine."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    # Load input data
    df = pd.read_csv(INPUT_PATH)

    # Enrich data with features
    enriched_df = enrich_job_postings(df)

    # Ensure output directory exists
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Save enriched dataset
    enriched_df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Enriched data saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
