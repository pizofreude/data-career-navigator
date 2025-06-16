"""
app/utils.py
Shared helper functions for the Streamlit dashboard.
"""
# Import necessary libraries
import os
from pathlib import Path
from configparser import ConfigParser
import duckdb
import pandas as pd


def get_motherduck_connection(db_name="data_career_navigator"):
    """
    Returns a DuckDB connection to the specified MotherDuck database.
    Requires MOTHERDUCK_TOKEN in environment or ~/.motherduck/credentials.
    """
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        cred_path = os.path.expanduser('~/.motherduck/credentials')
        if os.path.exists(cred_path):
            
            config = ConfigParser()
            config.read(cred_path)
            os.environ['MOTHERDUCK_TOKEN'] = config['default']['token']
    return duckdb.connect(f"md:{db_name}")

def run_query(sql, db_name="data_career_navigator"):
    """
    Runs a SQL query against MotherDuck and returns a pandas DataFrame.
    """
    con = get_motherduck_connection(db_name)
    return con.execute(sql).fetchdf()

def get_table_preview(table, n=10, db_name="data_career_navigator"):
    sql = f"SELECT * FROM {table} LIMIT {n}"
    return run_query(sql, db_name)
