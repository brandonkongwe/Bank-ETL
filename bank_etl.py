# Code for ETL operations on Largest Banks data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3
import numpy as np 
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_name = 'Largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
target_path = './Largest_banks_data.csv'
csv_path = './exchange_rate.csv'
log_file = './code_log.txt'


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + " : " + message + "\n")


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    # Extract the web page as text
    page = requests.get(url).text
    # Parse the text into an HTML object
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    # Extract all 'tbody' attributes of the HTML object 
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[1].find('a') is not None and '—' not in col[2]:
                a = col[1].find_all('a')[1]
                data_dict = {
                    'Name': a.contents[0],
                    'MC_USD_Billion': float(col[2].contents[0])
                }
                df1 = pd.DataFrame(data_dict, index = [0])
                df = pd.concat([df, df1], ignore_index = True)

    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    data = pd.read_csv(csv_path)
    exchange_dict = data.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x * exchange_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_dict['INR'], 2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
# print(df)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)
# print(df)
# print(df['MC_EUR_Billion'][4])

log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, target_path)

log_progress('Data saved to CSV file')

conn = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')

load_to_db(df, conn, table_name)

log_progress('Data loaded to Database as a table, Executing queries.')

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, conn)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, conn)

query_statement = f"SELECT NAME FROM {table_name} LIMIT 5"
run_query(query_statement, conn)

log_progress('Process Complete.')

conn.close()

log_progress('Server Connection closed.')
