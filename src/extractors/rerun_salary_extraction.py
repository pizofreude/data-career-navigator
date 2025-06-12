"""
File: src/extractors/rerun_salary_extraction.py
---------------------------------------------
This script reruns the salary extraction process on the enriched_jobs.csv file.
It prioritizes the header_text for salary extraction, falling back to the description if necessary.
Only run this script if needed to update salary information in the enriched_jobs.csv.
"""

# Import necessary libraries
import pandas as pd
from src.extractors.salary_extractor import SalaryETL

# Load the cleaned enriched_jobs.csv
df = pd.read_csv("../data/silver/enriched_jobs.csv")

etl = SalaryETL()

# Custom logic: prioritize header_text, fallback to description
def extract_salary_row(row):
    """
    Given a row of a DataFrame, extracts salary information from the header_text if it contains
    salary-related keywords; otherwise, falls back to the description. Returns a Series with the
    extracted salary information, or None if no salary information was found.

    Parameters
    ----------
    row : pandas Series
        A row of a DataFrame containing columns 'header_text' and 'description'.
    """
    # Try header_text first
    header_text = str(row.get('header_text', '') or '')
    description = str(row.get('description', '') or '')
    # If header_text contains salary-related keywords, use it; else use description
    salary_keywords = [
        "salary", "compensation", "pay", "base pay", "base salary", "annual", "per year", "per annum",
        "yearly", "monthly", "per month", "hourly", "per hour", "per week", "per day", "per diem",
        "remuneration", "wage", "package", "rate", "earn", "income"
    ]
    if any(kw in header_text.lower() for kw in salary_keywords):
        text = header_text
    else:
        text = description
    # Extract salary info
    results = etl.extractor.extract_salaries(text)
    if results:
        best = etl._select_best_salary_result(results)
        return pd.Series({
            'has_salary': True,
            'currency_raw': best.get('currency'),
            'min_salary_raw': best.get('min_salary'),
            'max_salary_raw': best.get('max_salary'),
            'single_salary_raw': best.get('single_salary'),
            'salary_period': best.get('period'),
            'salary_confidence': etl._calculate_confidence(best)
        })
    else:
        return pd.Series({
            'has_salary': False,
            'currency_raw': None,
            'min_salary_raw': None,
            'max_salary_raw': None,
            'single_salary_raw': None,
            'salary_period': None,
            'salary_confidence': None
        })

# Apply extraction row-wise
salary_df = df.apply(extract_salary_row, axis=1)

# Merge results back into original DataFrame
for col in salary_df.columns:
    df[col] = salary_df[col]

# Optionally, recalculate annualized USD columns if needed
df = etl.process_job_dataframe(df, text_column='description', include_title=True, title_column='title')

# Overwrite the original enriched_jobs.csv with the new results
df.to_csv("../data/silver/enriched_jobs.csv", index=False)