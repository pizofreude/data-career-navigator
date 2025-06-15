"""
File: src/extractors/skills_extractor.py

Module for extracting technical skills from job descriptions including:
- Programming Languages
- Python Libraries & Frameworks
- Analyst Tools & Platforms
- Cloud Services

Usage:
    from src.extractors.skills_extractor import extract_skills

    skills = extract_skills(description)
"""

# Import necessary libraries
import re

# Define your skill keyword lists
KEYWORDS_PROGRAMMING = [
    'sql', 'python', 'r', 'c', 'c#', 'javascript', 'js',  'java', 'scala', 'sas', 'matlab', 
    'c++', 'c/c++', 'perl', 'go', 'typescript', 'bash', 'html', 'css', 'php', 'powershell', 'rust', 
    'kotlin', 'ruby',  'dart', 'assembly', 'swift', 'vba', 'lua', 'groovy', 'delphi', 'objective-c', 
    'haskell', 'elixir', 'julia', 'clojure', 'solidity', 'lisp', 'f#', 'fortran', 'erlang', 'apl', 
    'cobol', 'ocaml', 'crystal', 'javascript/typescript', 'golang', 'nosql', 'mongodb', 't-sql', 'no-sql',
    'visual_basic', 'pascal', 'mongo', 'pl/sql', 'sass', 'vb.net', 'mssql'
]

KEYWORDS_LIBRARIES = [
    'scikit-learn', 'jupyter', 'theano', 'opencv', 'spark', 'nltk', 'mlpack', 'chainer', 'fann', 'shogun', 
    'dlib', 'mxnet', 'node.js', 'vue', 'vue.js', 'keras', 'ember.js', 'jse/jee',
]

KEYWORDS_ANALYST_TOOLS = [
    'excel', 'tableau', 'word', 'powerpoint', 'looker', 'powerbi', 'outlook', 'azure', 'jira', 'twilio', 
    'snowflake', 'shell', 'linux', 'sas', 'sharepoint', 'mysql', 'visio', 'git', 'mssql', 'powerpoints', 
    'postgresql', 'spreadsheets', 'seaborn', 'pandas', 'gdpr', 'spreadsheet', 'alteryx', 'github', 'postgres', 
    'ssis', 'numpy', 'power_bi', 'spss', 'ssrs', 'microstrategy', 'cognos', 'dax', 'matplotlib', 'dplyr', 
    'tidyr', 'ggplot2', 'plotly', 'esquisse', 'rshiny', 'mlr', 'docker', 'hadoop', 'airflow', 'redis', 
    'graphql', 'sap', 'tensorflow', 'node', 'asp.net', 'unix', 'jquery', 'pyspark', 'pytorch', 'gitlab', 
    'selenium', 'splunk', 'bitbucket', 'qlik', 'terminal', 'atlassian', 'unix/linux', 'linux/unix', 'ubuntu', 
    'nuix', 'datarobot',
]

KEYWORDS_CLOUD_TOOLS = [
    'aws', 'azure', 'gcp', 'snowflake', 'redshift', 'bigquery', 'aurora',
]

def normalize_skill(skill):
    """
    Normalize skill names to canonical forms for consistent extraction and analysis.
    Handles common variants, casing, dashes, and spaces.
    """
    if not isinstance(skill, str):
        return skill
    s = skill.strip().lower().replace('-', '').replace(' ', '').replace('_', '')
    # Map common Power BI variants
    if s in {'powerbi', 'power-bi', 'power bi', 'power_bi'}:
        return 'Power BI'
    # Add more mappings as needed
    return skill.strip()

def extract_skills(text):
    """
    Extract skills by category from a job description.

    Parameters:
        text (str): Job description text (from 'description' column).

    Returns:
        dict: A dictionary with extracted skills under each category.
    """
    if not isinstance(text, str):
        return {
            "programming_languages": [],
            "libraries": [],
            "analyst_tools": [],
            "cloud_platforms": []
        }

    text = text.lower()

    def extract_from_list(keywords):
        found = [kw for kw in keywords if re.search(r'\b' + re.escape(kw) + r'\b', text)]
        # Normalize all found skills
        return sorted(set([normalize_skill(kw) for kw in found]))

    return {
        "programming_languages": extract_from_list(KEYWORDS_PROGRAMMING),
        "libraries": extract_from_list(KEYWORDS_LIBRARIES),
        "analyst_tools": extract_from_list(KEYWORDS_ANALYST_TOOLS),
        "cloud_platforms": extract_from_list(KEYWORDS_CLOUD_TOOLS),
    }
