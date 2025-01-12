import psycopg2
from psycopg2 import sql, OperationalError

def connect_to_postgres(dbname, user, password, host="localhost", port=5432):
    """
    Connect to a PostgreSQL database and return the connection object.

    Parameters:
        dbname (str): Name of the database.
        user (str): Username for authentication.
        password (str): Password for authentication.
        host (str): Host where the database is running (default: "localhost").
        port (int): Port number for the database (default: 5432).

    Returns:
        connection: A psycopg2 connection object if successful.
    """
    try:
        # Establish the connection
        connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print(f"Successfully connected to the database: {dbname}")
        return connection

    except OperationalError as e:
        print(f"Error: Unable to connect to the database. Details: {e}")
        return None

def execute_query(connection, query):
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
                print("Query executed successfully.")
                return None
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
