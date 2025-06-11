"""
File: src/etl.py

ETL pipeline to enrich job listings with extracted insights:
- Salary estimates
- Experience level
- Technical skill categories
- Work type and employment type

Input:
    data/bronze/clean_jobs_with_header.csv

Output:
    data/silver/enriched_jobs.csv
"""

# Import necessary libraries
import sys
from pathlib import Path
import pandas as pd
# Import custom extractors
from extractors.salary_extractor import SalaryETL
from extractors.experience_extractor import categorize_experience
from extractors.skills_extractor import extract_skills
from extractors.job_type_extractor import extract_work_type, extract_employment_type
# Ensure parent directory is in sys.path for imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Define input/output paths
INPUT_PATH = Path("data/bronze/clean_jobs_with_header.csv")
OUTPUT_PATH = Path("data/silver/enriched_jobs.csv")

def enrich_job_postings(df):
    """
    Enriches a DataFrame with additional features extracted from job postings.

    Parameters:
        df (pd.DataFrame): Raw DataFrame with at least 'header_text', 'title' and 'description' columns.

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

    # Salary Extraction using SalaryETL with header_text as primary source, fallback to title+description
    salary_etl = SalaryETL()
    # First, try extracting salary from header_text only
    df_with_header = salary_etl.process_job_dataframe(
        df,
        text_column='header_text',
        include_title=False,
        title_column='title'  # not used, but required by signature
    )

    # Identify rows where salary was not extracted from header_text
    missing_salary_mask = ~df_with_header['has_salary'].fillna(False).astype(bool)

    # For those rows, fallback to title+description extraction
    if missing_salary_mask.any():
        df_fallback = salary_etl.process_job_dataframe(
            df_with_header.loc[missing_salary_mask],
            text_column='description',
            include_title=True,
            title_column='title'
        )
        # Update only the missing rows with fallback extraction results
        for col in [
            'has_salary', 'currency_raw', 'min_salary_raw', 'max_salary_raw',
            'single_salary_raw', 'salary_period', 'min_salary_annual_usd',
            'max_salary_annual_usd', 'avg_salary_annual_usd', 'salary_confidence']:
            df_with_header.loc[missing_salary_mask, col] = df_fallback[col]

    df = df_with_header

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

    # Work Type and Employment Type Extraction: use header_text first, fallback to title+description
    def get_work_type(row):
        header = str(row.get('header_text', '') or '').strip()
        if header:
            wt = extract_work_type(header, header)
            if wt != 'Not Specified':
                return wt
        return extract_work_type(str(row.get('title', '')), str(row.get('description', '')))

    def get_employment_type(row):
        header = str(row.get('header_text', '') or '').strip()
        if header:
            et = extract_employment_type(header, header)
            if et != 'Not Specified':
                return et
        return extract_employment_type(str(row.get('title', '')), str(row.get('description', '')))

    df['work_type'] = df.apply(get_work_type, axis=1)
    df['employment_type'] = df.apply(get_employment_type, axis=1)

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
