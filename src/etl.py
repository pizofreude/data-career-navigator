"""
File: src/etl.py

ETL pipeline to enrich job listings with extracted insights:
- Salary estimates
- Experience level
- Technical skill categories
- Work type and employment type
- Country from location
- Obfuscation cleaning

Input:
    data/bronze/clean_jobs_with_header.csv

Output:
    data/silver/enriched_jobs.csv
"""

# Import necessary libraries
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from collections import Counter, defaultdict
from sklearn.cluster import KMeans
import warnings
warnings.filterwarnings('ignore')
# Import custom extractors
from extractors.salary_extractor import SalaryETL
from extractors.experience_extractor import categorize_experience
from extractors.skills_extractor import extract_skills
from extractors.job_type_extractor import extract_work_type, extract_employment_type
from extractors.obfuscation_cleaner import drop_obfuscated_rows
from extractors.location_extractor import extract_country
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

    # Remove rows where all key columns (location, company) are obfuscated
    df = drop_obfuscated_rows(df, key_cols=['location', 'company'])

    # Extract country from 'location' column if present
    if 'location' in df.columns:
        print(f"Extracting country from location column for {len(df)} rows. This may take a while if geocoding is needed...")
        df['country'] = df['location'].apply(extract_country)
    else:
        df['country'] = 'Unknown'

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

def parse_skills_for_gold(df, skill_cols):
    # Parse semicolon-separated lists into Python lists
    for col in skill_cols:
        df[col] = df[col].fillna('').apply(lambda x: [s.strip() for s in x.split(';') if s.strip()])
    return df

def compute_clusters(df, skill_cols, n_clusters=5):
    # Build binary skill matrix for top N skills
    all_skills = df[skill_cols].apply(lambda row: sum(row, []), axis=1)
    flat_skills = [skill for sublist in all_skills for skill in sublist if skill]
    top_n = 50
    top_skills = [s for s, _ in Counter(flat_skills).most_common(top_n)]
    for skill in top_skills:
        df[f'skill_{skill}'] = all_skills.apply(lambda skills: int(skill in skills))
    skill_matrix = df[[f'skill_{skill}' for skill in top_skills]].values
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    clusters = kmeans.fit_predict(skill_matrix)
    df['cluster'] = clusters
    cluster_names = {
        0: "Business / Reporting Analysts",
        1: "Machine Learning Engineers / Researchers",
        2: "General Data Scientists / Analytics Engineers",
        3: "Data Engineers / Big-Data Specialists",
        4: "BI / Analytics Developers"
    }
    df['cluster_name'] = df['cluster'].map(cluster_names)
    return df, top_skills

def explode_job_skills(df, skill_cols):
    # Explode each skill category, then concatenate
    job_skills = []
    for idx, row in df.iterrows():
        job_id = row['id']
        for col in skill_cols:
            for skill in row[col]:
                if skill:
                    job_skills.append({
                        'job_id': job_id,
                        'skill': skill,
                        'skill_category': col
                    })
    return pd.DataFrame(job_skills)

def build_gold_tables(df, skill_cols, gold_dir):
    gold_dir.mkdir(parents=True, exist_ok=True)
    # 1. job_postings.parquet
    df.to_parquet(gold_dir / 'job_postings.parquet', index=False)
    # 2. skills.parquet
    all_skills = [skill for sublist in df[skill_cols].apply(lambda row: sum(row, []), axis=1) for skill in sublist if skill]
    skill_counts = Counter(all_skills)
    skills_df = pd.DataFrame([
        {'skill': s, 'frequency': c} for s, c in skill_counts.items()
    ])
    skills_df.to_parquet(gold_dir / 'skills.parquet', index=False)
    # 3. job_skills.parquet
    job_skills_df = explode_job_skills(df, skill_cols)
    job_skills_df.to_parquet(gold_dir / 'job_skills.parquet', index=False)
    # 4. companies.parquet
    companies = df.groupby('company').agg(
        job_count=('id', 'count'),
        median_salary=('avg_salary_annual_usd', 'median'),
        country=('country', lambda x: x.mode().iloc[0] if not x.mode().empty else None)
    ).reset_index()
    companies.to_parquet(gold_dir / 'companies.parquet', index=False)
    # 5. country_skill_counts.parquet
    country_skill = defaultdict(lambda: defaultdict(int))
    for _, row in df.iterrows():
        country = row['country']
        for col in skill_cols:
            for skill in row[col]:
                if skill:
                    country_skill[country][skill] += 1
    rows = []
    for country, skills in country_skill.items():
        for skill, count in skills.items():
            rows.append({'country': country, 'skill': skill, 'count': count})
    pd.DataFrame(rows).to_parquet(gold_dir / 'country_skill_counts.parquet', index=False)
    # 6. experience_skill_counts.parquet
    exp_skill = defaultdict(lambda: defaultdict(int))
    for _, row in df.iterrows():
        exp = row['experience_level']
        for col in skill_cols:
            for skill in row[col]:
                if skill:
                    exp_skill[exp][skill] += 1
    rows = []
    for exp, skills in exp_skill.items():
        for skill, count in skills.items():
            rows.append({'experience_level': exp, 'skill': skill, 'count': count})
    pd.DataFrame(rows).to_parquet(gold_dir / 'experience_skill_counts.parquet', index=False)
    # 7. salary_skill_stats.parquet
    skill_salary = []
    all_skills_series = df[skill_cols].apply(lambda row: sum(row, []), axis=1)
    for skill in skill_counts:
        mask = all_skills_series.apply(lambda skills: skill in skills)
        salaries = df.loc[mask, 'avg_salary_annual_usd'].dropna()
        if len(salaries) > 0:
            skill_salary.append({
                'skill': skill,
                'p25': np.percentile(salaries, 25),
                'median': np.percentile(salaries, 50),
                'p75': np.percentile(salaries, 75),
                'count': len(salaries)
            })
    pd.DataFrame(skill_salary).to_parquet(gold_dir / 'salary_skill_stats.parquet', index=False)

def main():
    """Main ETL routine."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    # Load input data
    df = pd.read_csv(INPUT_PATH)

    # Remove rows where all key columns (location, company) are obfuscated
    df = drop_obfuscated_rows(df, key_cols=['location', 'company'])

    # Extract country from 'location' column if present
    if 'location' in df.columns:
        print(f"Extracting country from location column for {len(df)} rows. This may take a while if geocoding is needed...")
        df['country'] = df['location'].apply(extract_country)
    else:
        df['country'] = 'Unknown'

    # Enrich data with features
    enriched_df = enrich_job_postings(df)

    # Ensure output directory exists
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Save enriched dataset
    enriched_df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Enriched data saved to: {OUTPUT_PATH}")

    # --- Gold ETL Process ---
    gold_dir = Path('data/gold')
    skill_cols = ['programming_languages', 'libraries', 'analyst_tools', 'cloud_platforms']
    df = pd.read_csv(OUTPUT_PATH)
    df = parse_skills_for_gold(df, skill_cols)
    df, _ = compute_clusters(df, skill_cols)
    build_gold_tables(df, skill_cols, gold_dir)
    print(f"✅ Gold-layer Parquet outputs saved to: {gold_dir}")

# Run the ETL pipeline
if __name__ == "__main__":
    main()
