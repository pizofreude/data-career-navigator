"""File: src/extractors/experience_extractor.py
This module contains a function to categorize job experience levels based on job titles and descriptions.
"""

# Import necessary libraries
import re

def categorize_experience(title: str, description: str) -> str:
    """
    Given a job title (from 'title' column) and job description (from 'description' column), return one of:
      - 'Entry-Level'
      - 'Mid-Level'
      - 'Senior'
      - 'Not Specified'

    1. Title-based rules (checked in order):
       • Senior keywords: 'senior', 'sr', 'lead', 'principal', 'manager', 'staff', 'head of'
         → return 'Senior'
       • 'Associate' (ambiguous): if 'associate' in title (without senior keywords), default to 'Entry-Level'
       • Mid keywords (excluding 'associate'): 'mid-level', 'intermediate', 'specialist'
         → return 'Mid-Level'
       • Entry keywords: 'junior', 'jr', 'entry-level', 'graduate', 'intern', 'trainee'
         → return 'Entry-Level'

    2. Description-based rules (if title had no match):
       a) Year-range pattern, e.g. "3–5 years":
          – If high ≤ 2 → 'Entry-Level'
          – If low ≥ 5 → 'Senior'
          – Else → 'Mid-Level'
       b) Single-year pattern, e.g. "3+ years" or "4 years":
          – If years ≤ 2 → 'Entry-Level'
          – If years ≥ 5 → 'Senior'
          – Else → 'Mid-Level'
       c) Keywords: 'recent graduate', 'entry-level role', 'fresh graduate'
          → return 'Entry-Level'

    3. Fallback: return 'Not Specified'
    """
    title_lower = title.lower()
    desc_lower = description.lower()

    # 1. Title-based rules
    senior_keywords = ["senior", "sr", "lead", "principal", "manager", "staff", "head of"]
    for kw in senior_keywords:
        if kw in title_lower:
            return "Senior"

    if "associate" in title_lower:
        return "Entry-Level"

    mid_keywords = ["mid-level", "intermediate", "specialist"]
    for kw in mid_keywords:
        if kw in title_lower:
            return "Mid-Level"

    entry_keywords = ["junior", "jr", "entry-level", "graduate", "intern", "trainee"]
    for kw in entry_keywords:
        if kw in title_lower:
            return "Entry-Level"

    # 2a. Year-range pattern (e.g. "3–5 years" or "3-5 years")
    range_pattern = re.compile(r"(\\d+)\\s*[–\\-]\\s*(\\d+)\\s*(?:years?|yrs?)")
    match_range = range_pattern.search(desc_lower)
    if match_range:
        low = int(match_range.group(1))
        high = int(match_range.group(2))
        if high <= 2:
            return "Entry-Level"
        if low >= 5:
            return "Senior"
        return "Mid-Level"

    # 2b. Single-year pattern (e.g. "3+ years" or "4 years")
    single_pattern = re.compile(r"(\\d+)\\+?\\s*(?:years?|yrs?)")
    match_single = single_pattern.search(desc_lower)
    if match_single:
        years = int(match_single.group(1))
        if years <= 2:
            return "Entry-Level"
        if years >= 5:
            return "Senior"
        return "Mid-Level"

    # 2c. Keywords in description
    if "recent graduate" in desc_lower or "entry-level role" in desc_lower or "fresh graduate" in desc_lower:
        return "Entry-Level"

    # 3. Fallback
    return "Not Specified"
