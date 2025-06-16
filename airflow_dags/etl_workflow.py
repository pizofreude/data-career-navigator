"""
File: airflow_dags/etl_workflow.py
----------------------------------
This script defines the Airflow DAG for the ETL workflow.
It includes tasks for updating exchange rates, data ingestion, scraping header text,
and running ETL processes both locally and on MotherDuck.
"""
# Import necessary libraries
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def manual_scrape_reminder():
    """Print a reminder to manually run the header text scraping script,
    since it requires a manual LinkedIn login."""
    print("Please run: python src/scrape_header_text_selenium.py (manual LinkedIn login required)")

def print_done():
    """
    Print a message indicating that the ETL workflow is complete.
    """

    print("ETL workflow complete!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    'data_career_navigator_etl',
    default_args=default_args,
    description='ETL workflow for Data Career Navigator',
    schedule_interval=None,  # Run manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

update_exchange_rate = BashOperator(
    task_id='update_exchange_rate',
    bash_command='git pull',
    dag=dag,
)

data_ingestion = BashOperator(
    task_id='run_data_ingestion',
    bash_command='python src/data_ingestion.py',
    dag=dag,
)

scrape_header = PythonOperator(
    task_id='scrape_header_reminder',
    python_callable=manual_scrape_reminder,
    dag=dag,
)

etl_local = BashOperator(
    task_id='run_etl_local',
    bash_command='python src/etl.py',
    dag=dag,
)

etl_motherduck = BashOperator(
    task_id='run_etl_motherduck',
    bash_command='python src/etl.py load_motherduck',
    dag=dag,
)

done = PythonOperator(
    task_id='done',
    python_callable=print_done,
    dag=dag,
)

# Define the task dependencies in the DAG
update_exchange_rate >> data_ingestion >> scrape_header >> etl_local >> etl_motherduck >> done
