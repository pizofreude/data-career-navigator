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

    # Check for explicit keywords in header_text style (semicolon or line separated)
    # Accepts: 'Remote', 'Hybrid', 'On-site', 'On site', etc.
    if 'remote' in text:
        return 'Remote'
    elif 'hybrid' in text:
        return 'Hybrid'
    elif 'on-site' in text or 'on site' in text or 'office-based' in text:
        return 'On-site'
    # Fallback to regex for more flexible matching
    elif re.search(r'\b(remote|work from home|wfh)\b', text):
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

    # Check for explicit keywords in header_text style (semicolon or line separated)
    # Accepts: 'Full-time', 'Part-time', 'Contract', etc.
    if 'full-time' in text or 'full time' in text or 'permanent' in text:
        return 'Full-time'
    elif 'part-time' in text or 'part time' in text:
        return 'Part-time'
    elif 'contract' in text or 'contractor' in text:
        return 'Contract'
    elif 'temporary' in text or 'temp' in text:
        return 'Temporary'
    elif 'internship' in text or 'intern' in text:
        return 'Internship'
    elif 'freelance' in text or 'freelancer' in text:
        return 'Freelance'
    # Fallback to regex for more flexible matching
    elif re.search(r'\b(full[- ]?time|permanent)\b', text):
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
