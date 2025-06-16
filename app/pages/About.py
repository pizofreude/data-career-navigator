"""
File: app/pages/About.py
-------------------------
Shows the About page for the Data Career Navigator application.
"""
# Import necessary libraries
import streamlit as st

# Set up the Streamlit page configuration
st.set_page_config(
    page_title="ℹ️ About | Data Career Navigator",
    page_icon="🧭",
    layout="wide"
)
st.title("ℹ️ About Data Career Navigator")

## Introduction text
st.markdown('''
## What is Data Career Navigator?

**Data Career Navigator** is an open-source analytics dashboard that provides real job market insights, salary trends, and skill demand for data-related careers worldwide. It leverages real job postings, advanced ETL pipelines, and interactive visualizations to help users:

- Explore global and regional demand for data skills
- Analyze salary distributions and trends
- Discover top companies and job archetypes
- Track changes in the data job market over time

## Key Features
- 🌍 **Global Coverage:** Data from multiple countries and regions
- 📊 **Advanced Visualizations:** Salary by skill, heatmaps, time trends, and more
- 🏢 **Company & Role Insights:** See which companies are hiring and for what roles
- ⏳ **Historical Trends:** Track how demand and salaries change over time
- 🦆 **Powered by DuckDB & MotherDuck:** Fast, scalable analytics on modern data infrastructure

## How It Works
- **Data Collection:** Automated ETL pipelines extract and clean job postings from various sources
- **Data Enrichment:** Skills, salaries, locations, and job types are extracted and standardized
- **Analytics & Visualization:** Data is loaded into DuckDB/MotherDuck and visualized with Streamlit and Plotly

## Open Source & Community
This project is open source and welcomes contributions! You can:
- ⭐ Star or fork the project on [GitHub](https://github.com/pizofreude/data-career-navigator)
- 🐛 [Report issues or request features](https://github.com/pizofreude/data-career-navigator/issues/new/choose)
- 🤝 Contribute code, data, or ideas

## Credits
- Developed by [@pizofreude](https://github.com/pizofreude) and contributors
- Built with [Streamlit](https://streamlit.io/), [DuckDB](https://duckdb.org/), [MotherDuck](https://motherduck.com/), and [Plotly](https://plotly.com/python/)

---

_If you have questions, feedback, or want to collaborate, please reach out via [GitHub Issues](https://github.com/pizofreude/data-career-navigator/issues/new/choose)._
''')
