"""
File: scripts/reextract_skills.py
Module for re-extracting and normalizing technical skills from job descriptions in the enriched jobs dataset.
Only updates the skill columns without modifying the rest of the dataset.
Use only if the skill extraction logic has changed or needs to be re-applied.
Otherwise, ETL will handle this automatically.
"""
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
from src.extractors.skills_extractor import extract_skills

# Load the enriched jobs dataset
input_path = "data/silver/enriched_jobs.csv"
df = pd.read_csv(input_path)

# Skill columns to update
skill_cols = [
    "programming_languages",
    "libraries",
    "analyst_tools",
    "cloud_platforms"
]

# Apply skill extraction and normalization to each row's description
# (Assumes a 'description' column exists)
def extract_and_update_skills(row):
    skills = extract_skills(row.get('description', ''))
    for col in skill_cols:
        row[col] = ";".join(skills[col])
    return row

df = df.apply(extract_and_update_skills, axis=1)

# Save the updated file (overwrite or create a new one)
df.to_csv("data/silver/enriched_jobs.csv", index=False)
print("Skill extraction and normalization complete. Output: data/silver/enriched_jobs.csv")
