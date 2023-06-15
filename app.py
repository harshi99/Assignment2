"""
Name: Harshitha Gokanakonda
Course: 6332 Cloud Computing and Big Data
Assignment Number: 02
"""
from flask import Flask, jsonify, request,render_template
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import os
import csv
import pyodbc
from datetime import datetime


app = Flask(__name__)


# Blob Storage configuration
blob_connection_string = 'DefaultEndpointsProtocol=https;AccountName=assdata1;AccountKey=WMGVFc5Btn/cWP1ErRdsoFKp+VOWcfM9r5C6uOYSod9jeunIxoThQp+A6ecG6R48CFywsaCRl/AZ+ASttwd/CA==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
container_name = 'assingment2'

# SQL configuration
server = 'harshi1.database.windows.net'
database = 'assdata2'
username = 'harshi'
password = 'Azure.123'
driver = '{ODBC Driver 18 for SQL Server};Server=tcp:harshi1.database.windows.net,1433;Database=assdata2;Uid=harshi;Pwd=Azure.123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

# Establish the database connection
connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
connection = pyodbc.connect(connection_string)

@app.route('/')
def index():
    return render_template('index.html')


# Function to execute SQL query
def execute_sql_query(query):
    with pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}') as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
    return columns, rows


#Route for SQL query execution
@app.route('/sql', methods=['POST'])
def execute_query():
    query = request.form['query']
    columns, rows = execute_sql_query(query)
    
    if len(rows) == 0:
        return 'No results found'
    
    result = []
    for row in rows:
        data = dict(zip(columns, row))
        result.append(data)
    return render_template('query_results.html', result=result)

if __name__ == '__main__':
    port = os.environ.get('PORT', 5000)
    app.run(debug=True)
    app.run(host='localhost', port=port)


