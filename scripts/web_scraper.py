import pandas as pd
import requests
import psycopg2
import logging
import os
from configs.connect_postgres import connect_to_postgres
from configs.query_postgres import create_table_if_not_exists, insert_data

# Configure logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "web_scraper.log")

if not os.access(log_file, os.W_OK):
    print(f"Warning: Log file '{log_file}' is not writable.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),   # Save logs to file
        logging.StreamHandler()          # Print logs to console
    ],
    force=True
)

def fetch_and_store_data(url: str, table_name: str, connection: psycopg2.extensions.connection):
    """
    Fetch JSON data from an API and store it in a PostgreSQL table.

    Parameters:
        url (str): The API endpoint.
        table_name (str): The PostgreSQL table name.
        connection: The PostgreSQL connection object.

    Returns:
        None
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to retrieve data from {url}. Error: {e}")
        return

    data = response.json()
    df = pd.json_normalize(data)

    if df.empty:
        logging.warning(f"No data received from {url}. Skipping...")
        return

    # Convert column names to valid SQL column names (remove spaces, special characters)
    df.columns = [col.lower().replace(" ", "_").replace("-", "_") for col in df.columns]

    # Ensure table exists before inserting
    try:
        create_table_if_not_exists(df, table_name, connection)
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        return

    # Convert DataFrame to NumPy array for insertion
    try:
        insert_data(df.to_numpy(), df.columns.tolist(), table_name, connection)
    except Exception as e:
        logging.error(f"Error inserting data into '{table_name}': {e}")
        return


if __name__ == "__main__":
    # List of data sources
    websites = {
        "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500": "taxi_trips",
        "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=500": "covid_cases",
        "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=500": "covid_vulnerability_index",
        "https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=500": "building_permits",
        "https://data.cityofchicago.org/resource/kn9c-c2s2.json?$limit=500": "census_data",
        "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=500": "transportation_trips",
        "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=500": "public_health_statistics"
    }

    conn = connect_to_postgres()
    
    if conn:
        for url, table_name in websites.items():
            logging.info(f"Fetching data from {url} and storing it in '{table_name}'...")
            fetch_and_store_data(url, table_name, conn)

        conn.close()
