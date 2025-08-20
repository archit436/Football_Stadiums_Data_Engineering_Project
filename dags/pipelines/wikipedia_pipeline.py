""" 
This file contains helper functions for the tasks that are a part of the Apache Airflow DAG for wikipedia data extraction.
"""
# Setup
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from geopy.geocoders import Nominatim
from datetime import datetime
from airflow.hooks.base import BaseHook

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

def clean_text(text: str) -> str:
    """
    Helper function to clean the text obtained from scraping the wikipedia page.
    """
    # Remove leading and trailing whitespace
    text = text.strip()
    # Remove the character in stadium names
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    # Remove citations from capacity.
    if text.find('[') != -1:
        text = text.split('[')[0]
    # Remove 'formerly' from capacity
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]
    # Remove any newline characters.
    text = text.replace('\n', ' ')
    return text

def extract_wikipedia_data(**kwargs) -> str:
    """
    Cumulative function that extracts data from wikipedia given the url.
    The output is a JSON string stored in a task instance on Airflow, to be used by downstream tasks.
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
        # Clean the text for each value.
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text),
        }
        # Add the values to the data list.
        data.append(values)
    
    # Convert the data into a JSON string and push to the inter-task interface of Airflow.
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    # Save the data to a CSV file for debugging purposes.
    data_df = pd.DataFrame(data)
    data_df.to_csv('data/extracted_data.csv', index=False)

    return "OK"

def get_lat_long(stadium, city, country) -> tuple:
    """
    Helper function to get latitude and longitude for a given stadium and country.
    This would be used in the data transformation function.
    Uses a persistent cache stored in the 'data/geocache.json' file.
    Defaults to city's location in case the stadium location is not found.
    """
    print(f"Getting lat/long for {stadium}, {city}, {country}")

    cache_file = 'data/geocache.json'

    # Load the cache if it exists.
    try:
        with open(cache_file, 'r') as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        cache = {}

    # Check cache for efficiency
    if stadium in cache:
        return cache[stadium]

    # If not found in cache, make a request to the geolocation API.
    geolocator = Nominatim(user_agent='FootballStadiumsDataPipeline/archtibhargava436@gmail.com', timeout=5)
    stadium_location = geolocator.geocode(f"{stadium}, {country}")
    time.sleep(1)  # Wait 1 second between requests.

    # If a valid response is received, then we use the location of the stadium.
    if stadium_location:
        cache[stadium] = (stadium_location.latitude, stadium_location.longitude)
    # Otherwise, we retrieve and use the location of the city.
    else:
        city_location = geolocator.geocode(f"{city}, {country}")
        cache[stadium] = (city_location.latitude, city_location.longitude)

    # Save the cache to a file for future use.
    with open(cache_file, 'w') as f:
        json.dump(cache, f)

    return cache[stadium]

def transform_wikipedia_data(**kwargs) -> str:
    """
    Transform the extracted Wikipedia data into a suitable format.
    The output will be a JSON string uploaded to Xcom for downstream tasks.
    """
    # Get the data from the previous task and deserialize it.
    json_rows = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    data = json.loads(json_rows)

    # Convert the data into a pandas DataFrame
    stadiums_df = pd.DataFrame(data)

    # Start by replacing the "NO_IMAGE" with wikipedia stock for no images.
    NO_IMAGE = "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/480px-No_image_available.svg.png"
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ["NO_IMAGE", "", None] else NO_IMAGE)

    # Convert the capacity numbers to integer.
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # Next, we enrich the data with latitude and longitude values.
    stadiums_df['lat_long'] = stadiums_df.apply(lambda x: get_lat_long(x['stadium'], x['city'], x['country']), axis=1)
    # Seperate the tuple value into two seperate columns.
    stadiums_df['Latitude'] = stadiums_df['lat_long'].apply(lambda x: x[0])
    stadiums_df['Longitude'] = stadiums_df['lat_long'].apply(lambda x: x[1])
    # Drop the lat long column
    stadiums_df = stadiums_df.drop(columns=['lat_long'])

    # Push to Xcom to be used by downstream tasks.
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    # Also save as csv file for debugging purposes.
    stadiums_df.to_csv('data/transformed_output.csv', index=False)

    return "OK"

def write_wikipedia_data(**kwargs) -> str:
    """
    Write the transformed Wikipedia data to a CSV file to use with Tableau
    The input will be from Xcom from the transformation task.

    """
    # Get the transformed data from the previous task and deserialize it from a JSON string.
    json_rows = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    data = json.loads(json_rows)

    # Convert to a Data Frame
    data_df = pd.DataFrame(data)

    # Define a file name with date and time stamp (UTC time)
    file_name = f"stadiums_data_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Output into a csv file.
    data_df.to_csv(f'data/{file_name}', index=False)

    return "OK"