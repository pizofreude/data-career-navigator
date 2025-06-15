"""
File: src/gold_etl_temp.py 
---------------------------
Manual script to generate gold-layer Parquet outputs from data/silver/enriched_jobs.csv.
This script mirrors the gold-layer logic in etl.py but is standalone for quick batch processing.
Outputs are saved to data/gold/.
Use only if the gold-layer logic has changed or needs to be re-applied.
Otherwise, ETL will handle this automatically.
"""
# Import necessary libraries
import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter, defaultdict
from sklearn.cluster import KMeans
import warnings
warnings.filterwarnings('ignore')

def parse_skills_for_gold(df, skill_cols):
    """
    Parse semicolon-separated lists into Python lists for specified columns.

    :param df: Input DataFrame
    :param skill_cols: List of columns to parse
    :return: Modified DataFrame with parsed columns
    """
    for col in skill_cols:
        df[col] = df[col].fillna('').apply(lambda x: [s.strip() for s in x.split(';') if s.strip()])
    return df

def compute_clusters(df, skill_cols, n_clusters=5):
    """
    Compute clusters based on binary skill features.

    :param df: Input DataFrame with jobs
    :param skill_cols: List of columns with semicolon-separated skill lists
    :param n_clusters: Number of clusters to compute (default: 5)
    :return: Modified DataFrame with cluster assignments and list of top skills
    """
    
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
    """
    Explode job skills into separate rows.

    :param df: Input DataFrame with jobs
    :param skill_cols: List of columns with semicolon-separated skill lists
    :return: DataFrame with exploded job skills
    """
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
    """
    Builds the gold layer tables from the input DataFrame.

    The following tables are created:

    1. job_postings.parquet: a copy of the input DataFrame
    2. skills.parquet: a table of skill names and their frequency in the job postings
    3. job_skills.parquet: a table of job postings and their associated skills
    4. companies.parquet: a table of company information, including the number of job postings and median salary
    5. country_skill_counts.parquet: a table of country-skill pairs and their frequency
    6. experience_skill_counts.parquet: a table of experience level-skill pairs and their frequency
    7. salary_skill_stats.parquet: a table of skill-salary statistics (p25, median, p75, and count)

    :param df: Input DataFrame with job postings
    :param skill_cols: List of columns with semicolon-separated skill lists
    :param gold_dir: Directory where the gold layer tables will be saved
    """
    gold_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(gold_dir / 'job_postings.parquet', index=False)
    all_skills = [skill for sublist in df[skill_cols].apply(lambda row: sum(row, []), axis=1) for skill in sublist if skill]
    skill_counts = Counter(all_skills)
    skills_df = pd.DataFrame([
        {'skill': s, 'frequency': c} for s, c in skill_counts.items()
    ])
    skills_df.to_parquet(gold_dir / 'skills.parquet', index=False)
    job_skills_df = explode_job_skills(df, skill_cols)
    job_skills_df.to_parquet(gold_dir / 'job_skills.parquet', index=False)
    companies = df.groupby('company').agg(
        job_count=('id', 'count'),
        median_salary=('avg_salary_annual_usd', 'median'),
        country=('country', lambda x: x.mode().iloc[0] if not x.mode().empty else None)
    ).reset_index()
    companies.to_parquet(gold_dir / 'companies.parquet', index=False)
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
    """
    Builds the gold layer tables from the enriched jobs dataset.

    The following steps are executed:

    1. Load the enriched jobs dataset from data/silver/enriched_jobs.csv
    2. Parse the skills columns for gold layer format
    3. Compute clusters based on binary skill features
    4. Build the following gold layer tables:

        a. job_postings.parquet: a copy of the input DataFrame
        b. skills.parquet: a table of skill names and their frequency in the job postings
        c. job_skills.parquet: a table of job postings and their associated skills
        d. companies.parquet: a table of company information, including the number of job postings and median salary
        e. country_skill_counts.parquet: a table of country-skill pairs and their frequency
        f. experience_skill_counts.parquet: a table of experience level-skill pairs and their frequency
        g. salary_skill_stats.parquet: a table of skill-salary statistics (p25, median, p75, and count)

    5. Save the gold layer tables to data/gold/ as Parquet files
    """
    enriched_path = Path('data/silver/enriched_jobs.csv')
    gold_dir = Path('data/gold')
    skill_cols = ['programming_languages', 'libraries', 'analyst_tools', 'cloud_platforms']
    df = pd.read_csv(enriched_path)
    df = parse_skills_for_gold(df, skill_cols)
    df, _ = compute_clusters(df, skill_cols)
    build_gold_tables(df, skill_cols, gold_dir)
    print(f"✅ Gold-layer Parquet outputs saved to: {gold_dir}")

# Ensure the script can be run as a standalone module
if __name__ == "__main__":
    main()
