""" 
This file contains helper functions for the tasks that are a part of the Apache Airflow DAG for wikipedia data extraction.
"""
# Setup
import requests
from bs4 import BeautifulSoup

# Function to get the wikipedia page corresponding to url.
# The output is html text from the page.
def get_wikipedia_page(url: str) -> str:
    """
    Extract data from a provided wikipedia page
    """
    # Status message
    print("Getting wikipedia data from .... ", url)

    # Try to get the data
    try:
        # Try with time out of 10 seconds and check for status.
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        # Print the error
        print(f"Error getting wikipedia data from {url}: {e}")
        return None

# Function to use wikipedia page to get data.
# Takes input as text from the page.