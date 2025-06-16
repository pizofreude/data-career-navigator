"""
File: src/gold_etl_temp.py 
---------------------------
Manual script to generate gold-layer Parquet outputs from data/silver/enriched_jobs.csv.
This script mirrors the gold-layer logic in etl.py but is standalone for quick batch processing.
Outputs are saved to data/gold/.
Use only if the gold-layer logic has changed or needs to be re-applied.
Otherwise, ETL will handle this automatically.
Usage:
For a complete ETL workflow, follow these steps:

1. Run the second-half of the ETL pipeline (silver → gold):
   ```bash
   python src/gold_etl_temp.py
   ```
   - This reads our enriched_jobs.csv data, and produces the gold-layer Parquet files in gold.

2. Load/append the gold-layer Parquet files to MotherDuck:
   ```bash
   python src/etl.py load_motherduck
   ```
   - This uploads (appends) the Parquet data to our MotherDuck database tables.

Summary:  
- Step 1: Run ETL and generate files.
- Step 2: Load those files into MotherDuck.

We can repeat step 2 as often as we want to append new data to MotherDuck, after each ETL run."""
# Import necessary libraries
import sys
import os
import warnings
from pathlib import Path
from configparser import ConfigParser
from collections import Counter, defaultdict
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import duckdb

warnings.filterwarnings('ignore')
# Add the parent directory to the system path for credentials import
cred_path = os.path.expanduser('~/.motherduck/credentials')
if os.path.exists(cred_path):
    config = ConfigParser()
    config.read(cred_path)
    os.environ['MOTHERDUCK_TOKEN'] = config['default']['token']

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
    Build binary skill matrix for top N skills and apply KMeans clustering.
    Clusters are named based on common data job roles.

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
    Appends to existing Parquet files and deduplicates for cumulative, non-duplicated gold-layer outputs.

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
    # Helper to append and deduplicate
    def append_and_dedupe(new_df, path, subset=None):
        """
        Appends new_df to existing Parquet file at path, deduplicating based on subset (if provided) or all columns (if not).
        If the file does not exist, simply writes the new_df to the Parquet file.
        :param new_df: DataFrame to append
        :param path: Path to the Parquet file
        :param subset: Optional list of columns to deduplicate on
        """
        if path.exists():
            old_df = pd.read_parquet(path)
            combined = pd.concat([old_df, new_df], ignore_index=True)
            if subset:
                combined = combined.drop_duplicates(subset=subset, keep="last")
            else:
                combined = combined.drop_duplicates(keep="last")
        else:
            combined = new_df
        combined.to_parquet(path, index=False)
    # 1. job_postings.parquet (dedupe by 'id')
    append_and_dedupe(df, gold_dir / 'job_postings.parquet', subset=['id'])
    # 2. skills.parquet (dedupe by 'skill')
    all_skills = [skill for sublist in df[skill_cols].apply(lambda row: sum(row, []), axis=1) for skill in sublist if skill]
    skill_counts = Counter(all_skills)
    skills_df = pd.DataFrame([
        {'skill': s, 'frequency': c} for s, c in skill_counts.items()
    ])
    append_and_dedupe(skills_df, gold_dir / 'skills.parquet', subset=['skill'])
    # 3. job_skills.parquet (dedupe by job_id+skill+skill_category)
    job_skills_df = explode_job_skills(df, skill_cols)
    append_and_dedupe(job_skills_df, gold_dir / 'job_skills.parquet', subset=['job_id', 'skill', 'skill_category'])
    # 4. companies.parquet (dedupe by company)
    companies = df.groupby('company').agg(
        job_count=('id', 'count'),
        median_salary=('avg_salary_annual_usd', 'median'),
        country=('country', lambda x: x.mode().iloc[0] if not x.mode().empty else None)
    ).reset_index()
    append_and_dedupe(companies, gold_dir / 'companies.parquet', subset=['company'])
    # 5. country_skill_counts.parquet (dedupe by country+skill)
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
    country_skill_df = pd.DataFrame(rows)
    append_and_dedupe(country_skill_df, gold_dir / 'country_skill_counts.parquet', subset=['country', 'skill'])
    # 6. experience_skill_counts.parquet (dedupe by experience_level+skill)
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
    exp_skill_df = pd.DataFrame(rows)
    append_and_dedupe(exp_skill_df, gold_dir / 'experience_skill_counts.parquet', subset=['experience_level', 'skill'])
    # 7. salary_skill_stats.parquet (dedupe by skill)
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
    skill_salary_df = pd.DataFrame(skill_salary)
    append_and_dedupe(skill_salary_df, gold_dir / 'salary_skill_stats.parquet', subset=['skill'])

def load_gold_to_motherduck(db_name="data_career_navigator"):
    """
    Loads all gold-layer Parquet files in data/gold/ into MotherDuck, appending to existing tables.
    Uses CREATE TABLE IF NOT EXISTS and INSERT INTO for append-only workflow.
    Requires MOTHERDUCK_TOKEN in environment.
    """
    gold_dir = Path("data/gold")
    parquet_files = [
        "job_postings.parquet",
        "skills.parquet",
        "job_skills.parquet",
        "companies.parquet",
        "country_skill_counts.parquet",
        "experience_skill_counts.parquet",
        "salary_skill_stats.parquet"
    ]
    table_names = [f.replace(".parquet", "") for f in parquet_files]
    # Connect to MotherDuck
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError("MOTHERDUCK_TOKEN environment variable not set.")
    con = duckdb.connect(f"md:{db_name}")
    for fname, tbl in zip(parquet_files, table_names):
        parquet_path = gold_dir / fname
        if tbl == 'job_postings':
            # Explicitly cast date_posted as DATE
            select_cols = """
                id, title, company, location, link, source,
                CAST(date_posted AS DATE) AS date_posted,
                work_type, employment_type, description, header_text, has_salary, currency_raw, min_salary_raw, max_salary_raw, single_salary_raw, salary_period, min_salary_annual_usd, max_salary_annual_usd, avg_salary_annual_usd, salary_confidence, experience_level, programming_languages, libraries, analyst_tools, cloud_platforms, country, cluster, cluster_name
            """
            create_sql = f"CREATE TABLE IF NOT EXISTS {tbl} AS SELECT {select_cols} FROM read_parquet('{parquet_path.as_posix()}') WHERE FALSE;"
            insert_sql = f"INSERT INTO {tbl} SELECT {select_cols} FROM read_parquet('{parquet_path.as_posix()}');"
        else:
            create_sql = f"CREATE TABLE IF NOT EXISTS {tbl} AS SELECT * FROM read_parquet('{parquet_path.as_posix()}') WHERE FALSE;"
            insert_sql = f"INSERT INTO {tbl} SELECT * FROM read_parquet('{parquet_path.as_posix()}');"
        con.execute(create_sql)
        con.execute(insert_sql)
        print(f"Appended {fname} to MotherDuck table: {tbl}")
    print(f"✅ All gold-layer tables loaded/appended to MotherDuck database: {db_name}")

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
    if len(sys.argv) > 1 and sys.argv[1] == "load_motherduck":
        load_gold_to_motherduck()
    else:
        main()
