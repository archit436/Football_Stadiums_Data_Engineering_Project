# Import necessary modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.wikipedia_pipeline import extract_wikipedia_data
from pipelines.wikipedia_pipeline import transform_wikipedia_data

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

# Extraction
# Define the extraction task using PythonOperator
# This task will call the extract_wikipedia_data function with the specified URL
# We assign the task to the Wikipedia Extraction DAG
extract_data_from_wikipedia = PythonOperator(
    task_id = 'extract_data_from_wikipedia',
    python_callable = extract_wikipedia_data,
    op_kwargs = {
        'url': 'https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity'
    },
    dag = dag
)

# Preprocessing
# Define the preprocessing task using PythonOperator
# This task will call the transform_wikipedia_data function to preprocess the extracted data
transform_wikipedia_data = PythonOperator(
    task_id = 'transform_wikipedia_data',
    python_callable = transform_wikipedia_data,
    dag = dag
)

# Writing


extract_data_from_wikipedia >> transform_wikipedia_data

