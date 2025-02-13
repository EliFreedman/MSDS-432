import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv
import os
import logging

# Load environment variables from .env file
load_dotenv()

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

def connect_to_postgres() -> psycopg2.extensions.connection:
    """
    Connect to a PostgreSQL database running in a Docker container.

    Returns:
        connection: A psycopg2 connection object if successful.
    """
    try:
        # Establish the connection
        connection = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB", "mydatabase"),
            user=os.getenv("POSTGRES_USER", "user"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432")
        )
        logger.info(f"Successfully connected to the database: {os.getenv('POSTGRES_DB', 'mydatabase')}")
        return connection

    except OperationalError as e:
        logger.error(f"Error: Unable to connect to the database. Details: {e}")
        return None
