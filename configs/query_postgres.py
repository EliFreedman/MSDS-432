import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import logging
import pandas as pd
import os
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()          # Print logs to console
    ],
    force=True
)
logger = logging.getLogger(__name__)

def execute_query(connection: psycopg2.extensions.connection, query: str) -> list:
    """
    Execute a SQL query on the connected database.

    Parameters:
        connection: A psycopg2 connection object.
        query (str): SQL query to be executed.

    Returns:
        results: Query results for SELECT statements, or None for others.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            if query.strip().lower().startswith("select"):
                results = cursor.fetchall()
                return results
            else:
                connection.commit()
                logger.info("Query executed successfully.")
                return None
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return None

def create_table_if_not_exists(df: pd.DataFrame, table_name: str, connection: psycopg2.extensions.connection):
    """Create a new table if it does not exist."""
    with connection.cursor() as cursor:
        columns = ", ".join([f'"{col}" TEXT' for col in df.columns])  # Store all as TEXT for flexibility
        create_table_query = sql.SQL(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                table_id SERIAL PRIMARY KEY,
                {columns}
            );
        """)
        cursor.execute(create_table_query)
        connection.commit()
        logger.info(f"Table '{table_name}' is ready.")

def insert_data(data: np.ndarray, columns: list, table_name: str, connection: psycopg2.extensions.connection):
    """Insert numpy array rows into PostgreSQL table."""
    with connection.cursor() as cursor:
        values = [tuple(row) for row in data]
        insert_query = sql.SQL(f"""
            INSERT INTO {table_name} ({", ".join([f'"{col}"' for col in columns])})
            VALUES %s;
        """)
        execute_values(cursor, insert_query, values)
        connection.commit()
        logger.info(f"Inserted {len(data)} rows into '{table_name}'.")