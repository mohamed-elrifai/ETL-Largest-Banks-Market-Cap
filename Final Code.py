# Code for ETL operations on world's largest banks data

# Importing the required libraries
import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    # Get current timestamp
    now = datetime.now()
    # Define timestamp format string
    format_string = "%Y-%m-%d %H:%M:%S"
    # Convert datetime object to string
    string_now = now.strftime(format_string)

    # Open the file in append mode
    with open(LOG_FILE, "a") as f:
        # Write your content to the file
        f.write(f"{string_now} : {message}" + "\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    # Access the response content
    page = requests.get(url).text
    # Scrape HTML to bs4 object
    soup = BeautifulSoup(page,'html.parser')
    
    table = soup.find_all('tbody')[0]
    rows = table.find_all('tr')
    rows = rows[1:] #Skip Headers

    bank_list = []
    MC_list = []
    for row in rows:
        cells = row.find_all('td')
        bank_list.append(cells[1].find_all('a')[1].text)
        MC_list.append(float(cells[2].text.strip()))
        
    df = pd.DataFrame(data= {table_attribs[0]: bank_list, table_attribs[1]: MC_list})
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # Read the CSV file
    rates_df = pd.read_csv(csv_path)
    # Extract currencies and rates
    currencies = rates_df.iloc[:, 0].tolist()
    rates = rates_df.iloc[:, 1].tolist()
    # Create dictionary
    exchange_rates = dict(zip(currencies, rates))

    # Add columns 
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_rates['GBP'] , 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates['EUR'] , 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates['INR'] , 2)

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query = pd.read_sql(query_statement, sql_connection)
    print(query)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


LOG_FILE = 'code_log.txt'
DATA_URL = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
EXCHANGE_RATE_PATH = 'exchange_rate.csv'
TARGET_CSV_PATH = './Largest_banks_data.csv'
TABLE_ATTRIBUTES = ['Name', 'MC_USD_Billion']
DB_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'
log_progress("Preliminaries complete. Initiating ETL process")

extracted_data = extract(DATA_URL, TABLE_ATTRIBUTES)
log_progress("Data extraction complete. Initiating Transformation process")

transformed_data = transform(extracted_data, EXCHANGE_RATE_PATH)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(transformed_data, TARGET_CSV_PATH)
log_progress("Data saved to CSV file")

# Initiate SQLite3 connection
sql_connection = sqlite3.connect(DB_NAME)
log_progress("SQL Connection initiated")

load_to_db(transformed_data, sql_connection, TABLE_NAME)
log_progress("Data loaded to Database as a table, Executing queries")

# contents of the entire table
query_statement = f"SELECT * FROM {TABLE_NAME}"
run_query(query_statement, sql_connection)
log_progress("Process Complete")

# average market capitalization of all the banks in Billion USD
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}"
run_query(query_statement, sql_connection)
log_progress("Process Complete")

# average market capitalization of all the banks in Billion USD
query_statement = f"SELECT Name from {TABLE_NAME} LIMIT 5"
run_query(query_statement, sql_connection)
log_progress("Process Complete")

# Close SQLite3 connection
log_progress("Server Connection closed")