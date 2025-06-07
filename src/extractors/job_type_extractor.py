"""
File: src/extractors/job_type_extractor.py
This module provides functions to extract work type and employment type from job titles and descriptions.
"""

# Import necessary library
import re

def extract_work_type(title: str, description: str) -> str:
    """
    Extracts the work type from the job title and description.
    Possible return values: 'Remote', 'Hybrid', 'On-site', 'Not Specified'
    """
    text = f"{title} {description}".lower()

    if re.search(r'\b(remote|work from home|wfh)\b', text):
        return 'Remote'
    elif re.search(r'\b(hybrid|flexible location|partially remote)\b', text):
        return 'Hybrid'
    elif re.search(r'\b(on[- ]?site|on site|office-based)\b', text):
        return 'On-site'
    else:
        return 'Not Specified'

def extract_employment_type(title: str, description: str) -> str:
    """
    Extracts the employment type from the job title and description.
    Possible return values: 'Full-time', 'Part-time', 'Contract', 'Temporary', 'Internship', 'Freelance', 'Not Specified'
    """
    text = f"{title} {description}".lower()

    if re.search(r'\b(full[- ]?time|permanent)\b', text):
        return 'Full-time'
    elif re.search(r'\b(part[- ]?time)\b', text):
        return 'Part-time'
    elif re.search(r'\b(contract|contractor)\b', text):
        return 'Contract'
    elif re.search(r'\b(temporary|temp)\b', text):
        return 'Temporary'
    elif re.search(r'\b(internship|intern)\b', text):
        return 'Internship'
    elif re.search(r'\b(freelance|freelancer)\b', text):
        return 'Freelance'
    else:
        return 'Not Specified'
