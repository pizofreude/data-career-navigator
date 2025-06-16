"""
File: app/pages/Status.py
-------------------------
This page provides up-to-date information about the status of job data collection,
system health, and useful statistics for Data Career Navigator.
"""
# Import necessary libraries
import streamlit as st
import pandas as pd
from utils import run_query
import plotly.express as px

# Set up the Streamlit page configuration
st.set_page_config(
    page_title="📊 Status | Data Career Navigator",
    page_icon="🧭",
    layout="wide"
)
st.title("📊 Status: Job Data Collection & System Health")
st.markdown("""
This page provides up-to-date information about the status of job data collection, system health, and useful statistics for Data Career Navigator.
""")

# --- Data Collection Status ---
st.header("Job Data Collection Status")
latest_job = run_query("SELECT MAX(date_posted) AS last_date FROM job_postings")
total_jobs = run_query("SELECT COUNT(*) AS total FROM job_postings")
total_companies = run_query("SELECT COUNT(DISTINCT company) AS companies FROM job_postings")
total_countries = run_query("SELECT COUNT(DISTINCT country) AS countries FROM job_postings")

# Remove time from latest_job
latest_job_date = pd.to_datetime(latest_job['last_date'][0]).date() if pd.notnull(latest_job['last_date'][0]) else "-"
col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest Job Posting", str(latest_job_date))
col2.metric("Total Job Postings", int(total_jobs['total'][0]))
col3.metric("Unique Companies", int(total_companies['companies'][0]))
col4.metric("Countries Covered", int(total_countries['countries'][0]))

# --- Monthly Job Collection Trend ---
st.header("Monthly Job Collection")
jobs_over_time = run_query("""
    SELECT date_posted FROM job_postings
""")
jobs_over_time["date_posted"] = pd.to_datetime(jobs_over_time["date_posted"])
jobs_over_time = jobs_over_time.dropna(subset=["date_posted"])  # Remove undefined dates
jobs_over_time["Month"] = jobs_over_time["date_posted"].dt.to_period("M").dt.to_timestamp()
monthly_counts = jobs_over_time.groupby("Month").size().reset_index(name="Jobs Collected")
# Smooth the line using a rolling average (window=3)
monthly_counts["Smoothed Jobs Collected"] = monthly_counts["Jobs Collected"].rolling(window=3, min_periods=1, center=True).mean()

# Create the line chart with Plotly Express
fig = px.line(
    monthly_counts,
    x="Month",
    y="Smoothed Jobs Collected",
    markers=True
)
fig.update_layout(
    title="",  # Explicitly set to empty string to avoid 'undefined' title
    yaxis_title="Jobs Collected",
    xaxis_title=None
)
fig.update_xaxes(
    tickformat="%m/%Y",
    dtick="M1"
)
st.plotly_chart(fig, use_container_width=True)

# --- Useful Stats ---
st.header("Useful Stats & System Info")
# Top 5 most recent companies
recent_companies = run_query("""
    SELECT company AS "Company Name", MAX(date_posted) AS "Most Recent Posting Date"
    FROM job_postings
    WHERE company IS NOT NULL
    GROUP BY company
    ORDER BY "Most Recent Posting Date" DESC
    LIMIT 5
""")
# Remove time from Most Recent Posting Date
recent_companies["Most Recent Posting Date"] = pd.to_datetime(recent_companies["Most Recent Posting Date"]).dt.date
st.subheader("Most Recent Companies Posting Jobs")
st.dataframe(recent_companies)

# Top 5 most common job titles
common_titles = run_query("""
    SELECT title AS "Job Title", COUNT(*) AS "Number of Postings"
    FROM job_postings
    GROUP BY title
    ORDER BY "Number of Postings" DESC
    LIMIT 5
""")
st.subheader("Most Common Job Titles")
st.dataframe(common_titles)

# Add more system health or ETL status info here as needed
st.markdown("---")
st.caption("Status page for Data Career Navigator. For more details, contact the [admin team](https://github.com/pizofreude/data-career-navigator/issues/new/choose).")
