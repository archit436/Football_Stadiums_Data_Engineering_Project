""" 
This file contains helper functions for the tasks that are a part of the Apache Airflow DAG for wikipedia data extraction.
"""
# Setup
import requests
from bs4 import BeautifulSoup
import pandas as pd

def get_wikipedia_page(url: str) -> str:
    """
    Function to extract data from a provided wikipedia page.
    The output is the html text from the page.
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

def get_wikipedia_data(html: str) -> dict:
    """
    Function to use wikipedia page to get data.
    Takes input as text from the page.
    The output is a list of table rows from the page.
    """

    # Parse the html content using BeautifulSoup
    # We look for the second table with the specific class and extract all table row items.
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all('table', {'class': 'wikitable'})[1]
    table_rows = table.find_all('tr')

    return table_rows

def extract_wikipedia_data(**kwargs) -> list:
    """
    Cumulative function that extracts data from wikipedia given the url.
    """
    # Extract the URL and run the functions above.
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    # Now we convert the data into a suitable format.
    data = []
    for i in range(1, len(rows)):
        # We extract all the table data items for this row.
        tds = rows[i].find_all('td')
        # Extract the values based on HTML code of the page.
        values = {
            'rank': i,
            'stadium': tds[0].text,
            'capacity': tds[1].text,
            'region': tds[2].text,
            'country': tds[3].text,
            'city': tds[4].text,
            'images': tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': tds[6].text,      
        }
        # Add the values to the data list.
        data.append(values)
    
    # Put the data into a dataframe and then csv.
    data_df = pd.DataFrame(data)
    data_df.to_csv('/opt/airflow/data/output.csv', index=False)
    return data