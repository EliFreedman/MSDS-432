import pandas as pd
import requests


def fetch_and_convert_to_excel(url, table_name):
    """
    Take data from a table in JSON format given the URL.

    Parameters:
        url (str): The URL of the website to be scraped.
        table_name (str): The name of the excel file that will be output
    
    Returns:
        None
    """
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Normalize the JSON data (handle missing or inconsistent fields)
        df = pd.json_normalize(data)
        
        # Save the DataFrame to an Excel file
        excel_file_path = f"../data/{table_name}.xlsx"
        df.to_excel(excel_file_path, index=False)
        print(f"Data has been saved to {excel_file_path}")
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")

if __name__ == "__main__":

    websites = {
        "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500": "Taxi Trips (2013-2023)",
        "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=500": "COVID-19 Cases, Tests, and Deaths by ZIP Code - Historical",
        "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=500": "Chicago COVID-19 Community Vulnerability Index (CCVI)",
        "https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=500": "Building Permits",
        "https://data.cityofchicago.org/resource/kn9c-c2s2.json?$limit=500": "Census Data - Selected socioeconomic indicators in Chicago, 2008 – 2012",
        "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=500": "Transportation Network Providers - Trips (2018 - 2022)",
        "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=500": "Public Health Statistics - Selected public health indicators by Chicago community area - Historical"
    }

    for url in websites:
        table_name = websites[url]
        print(f"Fetching data from {url}...")
        fetch_and_convert_to_excel(url, table_name)