"""
File: app/Dashboard.py
-------------------------
Streamlit dashboard for interactive analytics on data career navigator gold-layer tables in MotherDuck.
"""
# Import necessary libraries
import json
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils import run_query, get_table_preview
import requests

# Set up the Streamlit page configuration
# Set page title, icon, and layout
st.set_page_config(
    page_title="🌍 Dashboard | Data Career Navigator",
    page_icon="🧭",  # Compass emoji as favicon
    layout="wide"
)
st.title("🌍 Data Career Navigator: Global Data Careers Analytics")
st.markdown("""
Explore real job market data, skills, salaries, and trends for data-related careers worldwide. Powered by MotherDuck and DuckDB.
""")

# Sidebar: Table selector and preview
# Map internal table names to descriptive display names
TABLE_DISPLAY_NAMES = {
    "job_postings": "Job Postings (All)",
    "skills": "Skills (Global)",
    "job_skills": "Job-Skill Links",
    "companies": "Companies",
    "country_skill_counts": "Skills by Country",
    "experience_skill_counts": "Skills by Experience Level",
    "salary_skill_stats": "Skill Salary Stats",
}
# Reverse mapping for lookup
DISPLAY_TO_TABLE = {v: k for k, v in TABLE_DISPLAY_NAMES.items()}

# --- FILTERS ---
# Load filter options (from previous queries)
skill_options = [
    'Power BI','airflow','alteryx','assembly','atlassian','aurora','aws','azure','bash','bigquery','bitbucket','c','clojure','cognos','crystal','css','dart','dax','delphi','docker','dplyr','excel','gcp','gdpr','git','github','gitlab','go','golang','graphql','hadoop','html','java','javascript','jira','js','julia','jupyter','keras','kotlin','linux','looker','matlab','matplotlib','microstrategy','mongodb','mssql','mxnet','mysql','nltk','no-sql','node','node.js','nosql','numpy','opencv','outlook','pandas','perl','php','pl/sql','plotly','postgres','postgresql','powerpoint','powershell','pyspark','python','pytorch','qlik','r','redis','redshift','ruby','rust','sap','sas','scala','scikit-learn','seaborn','selenium','sharepoint','shell','snowflake','spark','splunk','spreadsheet','spreadsheets','spss','sql','ssis','ssrs','swift','t-sql','tableau','tensorflow','terminal','typescript','unix','unix/linux','vb.net','vba','visio','word'
]
# Dynamically load company options from the database
company_df = run_query('SELECT DISTINCT company FROM companies ORDER BY company')
company_options = company_df['company'].dropna().astype(str).tolist()
work_type_options = ["Hybrid", "Not Specified", "On-site", "Remote"]
employment_type_options = ["Contract", "Full-time", "Internship", "Not Specified", "Part-time", "Temporary"]

# Add a "All" option to each filter
st.sidebar.markdown("---")
st.sidebar.header("Filters")
selected_skill = st.sidebar.selectbox("Filter by Skill:", ["All"] + skill_options)
selected_company = st.sidebar.selectbox("Filter by Company:", ["All"] + company_options)
selected_work_type = st.sidebar.selectbox("Filter by Work Type:", ["All"] + work_type_options)
selected_employment_type = st.sidebar.selectbox("Filter by Employment Type:", ["All"] + employment_type_options)

# Display the selected filters for table preview
table_display = st.sidebar.selectbox(
    "Select a gold-layer table to preview:", list(TABLE_DISPLAY_NAMES.values())
)
table = DISPLAY_TO_TABLE[table_display]
preview = get_table_preview(table, n=10)
# Rename columns for preview
preview = preview.rename(columns={
    'date_posted': 'Date Posted',
    'n': 'Job Count',
    'company': 'Company',
    'country': 'Country',
    'cluster_name': 'Job Archetype',
    'median': 'Median Salary (USD)',
    'p25': '25th Percentile (USD)',
    'p75': '75th Percentile (USD)',
    'avg_salary_annual_usd': 'Average Salary (USD)',
    'job_count': 'Job Count',
})
st.sidebar.dataframe(preview)

# Improved custom CSS for table alignment: index and first column left, rest right
st.markdown("""
    <style>
    /* Index column left-aligned */
    .stDataFrame thead tr th.row_heading, .stDataFrame tbody tr th.row_heading {
        text-align: left !important;
    }
    /* First data column left-aligned */
    .stDataFrame thead tr th.col_heading.level0.col0, .stDataFrame tbody tr td.col0 {
        text-align: left !important;
    }
    /* All other columns right-aligned */
    .stDataFrame thead tr th:not(.row_heading):not(.col0), .stDataFrame tbody tr td:not(.col0) {
        text-align: right !important;
    }
    .stDataFrame thead tr th, .stDataFrame tbody tr td {
        vertical-align: middle !important;
    }
    </style>
""", unsafe_allow_html=True)


# Main: Interactive analytics
tab1, tab2, tab3, tab4 = st.tabs([
    "Job Market Overview", "Skills & Demand", "Salary Insights", "Geography & Companies"
])

# Filter the job_postings table based on selections
filtered_job_postings = run_query(
    f"""
    SELECT * FROM job_postings
    WHERE
        ('{selected_skill}' = 'All' OR cluster_name LIKE '%{selected_skill}%') AND
        ('{selected_company}' = 'All' OR company = '{selected_company}') AND
        ('{selected_work_type}' = 'All' OR work_type = '{selected_work_type}') AND
        ('{selected_employment_type}' = 'All' OR employment_type = '{selected_employment_type}')
    """
)

with tab1:
    st.subheader("Job Market Overview")
    total_jobs = len(filtered_job_postings)
    archetype_counts = filtered_job_postings.groupby('cluster_name').size().reset_index(name='Job Count')
    archetype_counts = archetype_counts.rename(columns={'cluster_name': 'Job Archetype'})
    archetype_counts = archetype_counts.sort_values(by="Job Count", ascending=False)
    st.metric("Total Job Postings", total_jobs)
    st.bar_chart(archetype_counts.set_index('Job Archetype'))
    date_counts = filtered_job_postings.groupby('date_posted').size().reset_index(name='Job Count')
    date_counts = date_counts.rename(columns={'date_posted': 'Date Posted'})
    date_counts = date_counts.sort_values(by="Date Posted", ascending=False).head(30)
    # Format 'Date Posted' to show only date (no time)
    if 'Date Posted' in date_counts.columns:
        date_counts['Date Posted'] = pd.to_datetime(date_counts['Date Posted']).dt.date
    st.dataframe(date_counts)
    # --- Time Trend Visualization ---
    st.markdown("#### 📈 Job Postings Over Time")
    time_trend = filtered_job_postings.groupby('date_posted').size().reset_index(name='Job Count')
    time_trend['date_posted'] = pd.to_datetime(time_trend['date_posted'])
    time_trend = time_trend.sort_values('date_posted')
    fig_time = px.line(
        time_trend, x='date_posted', y='Job Count',
        title='Job Postings Trend Over Time',
        labels={'date_posted': 'Date Posted', 'Job Count': 'Number of Job Postings'}
    )
    st.plotly_chart(fig_time, use_container_width=True)

with tab2:
    st.subheader("Skills & Demand")
    top_skills = run_query("SELECT skill AS 'Skill', frequency AS 'Frequency' FROM skills ORDER BY frequency DESC LIMIT 20")
    # Ensure top skills are sorted by Frequency descending (left =highest)
    top_skills_sorted = top_skills.sort_values(by="Frequency", ascending=False)
    
    fig_skills = go.Figure(
        data=[go.Bar(
            x=top_skills_sorted['Skill'],
            y=top_skills_sorted['Frequency'],
            marker_color='rgba(0,123,255,0.7)'
        )]
    )
    fig_skills.update_layout(
        xaxis_title="Skill",
        yaxis_title="Frequency",
        title="Top 20 Skills by Frequency",
        xaxis_tickangle=-45,
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=40, r=20, t=40, b=100),
    )
    st.plotly_chart(fig_skills, use_container_width=True)
    us_skills = run_query("SELECT skill AS 'Skill', count AS 'Job Count' FROM country_skill_counts WHERE country='United States' ORDER BY count DESC LIMIT 10")
    st.dataframe(us_skills)
    # --- Improved Heatmap: Skill vs. Country ---
    st.markdown("#### 🌎 Skill Demand Heatmap by Country")
    heatmap_data = run_query("SELECT country, skill, count FROM country_skill_counts WHERE count > 0")
    # Limit to top 10 countries and top 15 skills by total job count
    top_countries = heatmap_data.groupby('country')['count'].sum().nlargest(10).index.tolist()
    top_skills_heat = heatmap_data.groupby('skill')['count'].sum().nlargest(15).index.tolist()
    filtered_heatmap = heatmap_data[
        heatmap_data['country'].isin(top_countries) & heatmap_data['skill'].isin(top_skills_heat)
    ]
    pivot_heatmap = filtered_heatmap.pivot_table(index='country', columns='skill', values='count', fill_value=0)
    # Sort axes for readability
    pivot_heatmap = pivot_heatmap.loc[sorted(pivot_heatmap.index), sorted(pivot_heatmap.columns)]
    fig_heatmap = px.imshow(
        pivot_heatmap,
        labels=dict(x="Skill", y="Country", color="Job Count"),
        aspect="auto",
        title="Skill Demand Heatmap by Country",
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)

with tab3:
    st.subheader("Salary Insights")
    salary_stats = run_query("SELECT skill AS 'Skill', median AS 'Median Salary (USD)', p75 AS '75th Percentile (USD)', p25 AS '25th Percentile (USD)', count AS 'Job Count' FROM salary_skill_stats ORDER BY median DESC LIMIT 20")
    st.dataframe(salary_stats)
    archetype_salary = run_query("SELECT cluster_name AS 'Job Archetype', ROUND(AVG(avg_salary_annual_usd),0) AS 'Average Salary (USD)' FROM job_postings GROUP BY cluster_name ORDER BY AVG(avg_salary_annual_usd) DESC")
    st.dataframe(archetype_salary)
    # --- Salary by Skill Visualization ---
    st.markdown("#### 💸 Salary Distribution by Skill")
    salary_dist = run_query("SELECT skill, median, p25, p75 FROM salary_skill_stats WHERE median > 0 ORDER BY median DESC LIMIT 30")
    fig_salary = go.Figure()
    fig_salary.add_trace(go.Box(
        y=salary_dist['median'],
        x=salary_dist['skill'],
        name='Median',
        marker_color='royalblue',
        boxmean=True
    ))
    fig_salary.update_layout(
        title="Salary Distribution by Skill (Top 30)",
        xaxis_title="Skill",
        yaxis_title="Salary (USD)",
        xaxis_tickangle=-45,
        showlegend=False,
        margin=dict(l=40, r=20, t=40, b=100),
    )
    st.plotly_chart(fig_salary, use_container_width=True)
    # --- Salary Time Trend ---
    st.markdown("#### ⏳ Median Salary Trend Over Time")
    salary_time = run_query("SELECT date_posted, AVG(avg_salary_annual_usd) AS avg_salary FROM job_postings WHERE avg_salary_annual_usd > 0 GROUP BY date_posted ORDER BY date_posted")
    salary_time['date_posted'] = pd.to_datetime(salary_time['date_posted'])
    fig_salary_time = px.line(
        salary_time, x='date_posted', y='avg_salary',
        title='Average Salary Trend Over Time',
        labels={'date_posted': 'Date Posted', 'avg_salary': 'Average Salary (USD)'}
    )
    st.plotly_chart(fig_salary_time, use_container_width=True)

with tab4:
    st.subheader("Geography & Companies")
    country_counts = run_query("SELECT country AS 'Country', COUNT(*) AS 'Job Count' FROM job_postings GROUP BY country ORDER BY COUNT(*) DESC LIMIT 1000")
    st.dataframe(country_counts)
    # Global map visualization (Modern Design)
    st.markdown("#### Global Job Postings Map")
    fig = px.choropleth(
        country_counts,
        locations="Country",
        locationmode="country names",
        color="Job Count",
        hover_name="Country",
        color_continuous_scale=px.colors.sequential.Viridis,
        projection="natural earth",
        title="Global Distribution of Data Job Postings",
    )
    fig.update_traces(
        hovertemplate='<b>%{hovertext}</b><br>Job Counts = %{z}<extra></extra>'
    )
    fig.update_geos(
        showocean=True,
        oceancolor='LightBlue',
        landcolor='LightGray',
        showland=True,
        showcountries=True,
        countrycolor='white',
        subunitcolor='white',
        showframe=False,
        showcoastlines=True,
        coastlinecolor='white',
        projection_type='natural earth',
    )
    fig.update_coloraxes(
        colorbar_title="Job Counts",
        colorbar_thickness=15,
        colorbar_title_side='right',
        colorbar_title_font=dict(size=14, family='Arial', color='black'),
    )
    fig.update_layout(
        title={
            'text': '<b>Global Distribution of Data Job Postings</b>',
            'x': 0.5,
            'xanchor': 'center',
            'font': dict(size=22)
        },
        margin={"r":0,"t":50,"l":0,"b":0},
        geo_bgcolor='rgba(0,0,0,0)',
    )
    st.plotly_chart(fig, use_container_width=True)
    company_stats = run_query("SELECT company AS 'Company', job_count AS 'Job Count', median_salary AS 'Median Salary (USD)' FROM companies ORDER BY job_count DESC LIMIT 20")
    st.dataframe(company_stats)

st.markdown("---")
st.caption("Built with Streamlit, DuckDB, and MotherDuck. Share this dashboard with anyone interested in data careers!")
