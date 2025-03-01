import pandas as pd
import requests
import psycopg2
import logging
import os
from configs.query_postgres import create_table_if_not_exists, insert_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
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