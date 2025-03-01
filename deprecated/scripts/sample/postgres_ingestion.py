import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(".."))
from configs.connect_postgres import connect_to_postgres


def upload_csv_to_postgres(connection, table_name, csv_file):
    """
    Upload data from a CSV file to a PostgreSQL table.

    Parameters:
        connection: A psycopg2 connection object.
        table_name (str): Name of the target table in the database.
        csv_file (str): Path to the CSV file.

    Returns:
        None
    """
    try:
        # Read data from the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)
        print(f"Read {len(df)} rows from {csv_file}.")

        # Create a table dynamically based on the DataFrame schema
        with connection.cursor() as cursor:
            columns = ", ".join([f"{col} TEXT" for col in df.columns])
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns}
            );
            """
            cursor.execute(create_table_query)
            connection.commit()
            print(f"Table '{table_name}' created successfully.")

        # Insert data into the table
        with connection.cursor() as cursor:
            for _, row in df.iterrows():
                values = tuple(row)
                placeholders = ", ".join(["%s"] * len(row))
                insert_query = f"""
                INSERT INTO {table_name} VALUES ({placeholders});
                """
                cursor.execute(insert_query, values)
            connection.commit()
            print(f"Data uploaded to '{table_name}' successfully.")

    except Exception as e:
        print(f"Error uploading data: {e}")

if __name__ == "__main__":
    # Database connection
    conn = connect_to_postgres()

    if conn:
        # Specify the table name and CSV file path
        TABLE_NAME = "building_permits"
        CSV_FILE_PATH = "../data/Building_Permits.csv"

        # Upload data to the PostgreSQL table
        upload_csv_to_postgres(conn, TABLE_NAME, CSV_FILE_PATH)

        # Close the connection
        conn.close()
        print("Database connection closed.")