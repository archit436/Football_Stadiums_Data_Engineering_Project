# Import necessary modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.wikipedia_pipeline import get_wikipedia_page

# Define the Airflow DAG for the Wikipedia data extraction workflow
dag = DAG(
    dag_id = 'wikipedia_flow',
    default_args = {
        "owner": "airflow",
        "start_date": datetime(2025,8,16),
    },
    schedule = None,  # No schedule, this DAG is triggered manually
    catchup = False   # Do not perform backfill for missed runs
)

# Define the extraction task using PythonOperator
# This task will call the get_wikipedia_page function with the specified URL
# We assign the task to the Wikipedia Extraction DAG
extract_data_from_wikipedia = PythonOperator(
    task_id = 'extract_data_from_wikipedia',
    python_callable = get_wikipedia_page,
    op_kwargs = {
        'url': 'https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity'
    },
    dag = dag
)

# Preprocessing
# Write to Azure Data Lake