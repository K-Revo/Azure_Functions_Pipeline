import logging
import azure.functions as func
import requests
import pandas as pd
import pyodbc
import os
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import json

# Initialize the Function App
app = func.FunctionApp()

# CHANGE: @app.schedule -> @app.timer_trigger
@app.timer_trigger(schedule="0 0 8 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False) 
def DailyFetch(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is running late!')

    logging.info('Pipeline started...')
    
    # --- CONFIGURATION ---
    sql_conn_str = os.environ.get("SQL_CONNECTION_STRING")
    storage_conn_str = os.environ.get("AzureWebJobsStorage")
    container_name = "raw-data"
    
    api_url = "https://jsonplaceholder.typicode.com/users"

    try:
        # --- 1. FETCH DATA ---
        logging.info(f"Fetching data from {api_url}...")
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json() 
    
        # Save to Storage
        filename = f"users_data_{datetime.now().strftime('%Y-%m-%d')}.json"
        blob_service_client = BlobServiceClient.from_connection_string(storage_conn_str)
        
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            container_client.create_container()
            
        blob_client = container_client.get_blob_client(filename)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        logging.info(f"Saved {filename} to Blob Storage.")

        # --- 2. TRANSFORM ---
        df = pd.json_normalize(data)

        # --- 3. INSERT TO SQL ---
        logging.info("Connecting to SQL Database...")
        with pyodbc.connect(sql_conn_str) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='TestUsers' AND xtype='U')
                CREATE TABLE TestUsers (
                    UserID INT,
                    FullName NVARCHAR(100),
                    Email NVARCHAR(100),
                    City NVARCHAR(50),
                    IngestionDate DATETIME DEFAULT GETDATE()
                )
            """)

            for index, row in df.iterrows():
                cursor.execute(
                    "INSERT INTO TestUsers (UserID, FullName, Email, City) VALUES (?, ?, ?, ?)",
                    row['id'], 
                    row['name'], 
                    row['email'], 
                    row['address.city'] 
                )
            
            conn.commit()
            logging.info("Success! 10 Test Users inserted into SQL.")

    except Exception as e:
        logging.error(f"PIPELINE FAILED: {str(e)}")
        raise e