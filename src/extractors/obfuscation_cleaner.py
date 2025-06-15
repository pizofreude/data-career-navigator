"""
File: src/extractors/obfuscation_cleaner.py
----------------------------------------------
Utility to detect and filter out rows where all columns except 'link' are obfuscated
(e.g., only asterisks or empty).
"""

# Import necessary libraries
import re
import pandas as pd


def is_obfuscated(val):
    """
    Returns True if the value is obfuscated, otherwise False.
    A value is considered obfuscated if it is NaN, or if it is a string
    containing only non-alphanumeric characters (after removing whitespace).
    """
    if pd.isna(val):
        return True
    if not isinstance(val, str):
        return False
    # Remove whitespace and common obfuscation chars
    cleaned = re.sub(r'[^a-zA-Z0-9]', '', val)
    return cleaned == ''

def drop_obfuscated_rows(df, key_cols=None):
    """
    Drops rows where all key columns are obfuscated or empty.
    By default, uses ['location', 'company'] as key columns.
    """
    if key_cols is None:
        key_cols = ['location', 'company']
    mask = df[key_cols].apply(lambda col: col.map(is_obfuscated)).all(axis=1)
    dropped = mask.sum()
    print(f"Dropping {dropped} rows where all key columns are obfuscated out of {len(df)}")
    return df[~mask].copy()

def drop_rows_with_any_obfuscated(df, cols=None):
    """
    Drops rows where any column (or any of the specified columns) is obfuscated (e.g., contains only non-alphanumeric characters, asterisks, or is NaN).
    If cols is None, checks all columns.
    """
    if cols is None:
        cols = df.columns
    mask = df[cols].applymap(is_obfuscated).any(axis=1)
    dropped = mask.sum()
    print(f"Dropping {dropped} rows where any column is obfuscated out of {len(df)}")
    return df[~mask].copy()
