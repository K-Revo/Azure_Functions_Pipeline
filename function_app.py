import logging
import azure.functions as func
import requests
import pandas as pd
import pyodbc
import os
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import json

app = func.FunctionApp()

@app.schedule(schedule="0 0 8 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def DailyFetch(myTimer: func.TimerRequest) -> None:
    logging.info('Pipeline started...')
    
    # --- CONFIGURATION ---
    # We get these from local.settings.json (locally) or App Settings (in Azure)
    sql_conn_str = os.environ["SQL_CONNECTION_STRING"]
    storage_conn_str = os.environ["AzureWebJobsStorage"] # Using the main storage account
    api_url = "https://api.coindesk.com/v1/bpi/currentprice.json" # Example API
    container_name = "pf2134"

    try:
        # 1. FETCH DATA (API)
        logging.info(f"Fetching data from {api_url}...")
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        
        # Generate a filename based on today's date
        filename = f"crypto_data_{datetime.now().strftime('%Y-%m-%d')}.json"

        # 2. SAVE TO STORAGE (Bronze Layer)
        logging.info("Saving to Blob Storage...")
        blob_service_client = BlobServiceClient.from_connection_string(storage_conn_str)
        
        # Create container if it doesn't exist (Safety check)
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            container_client.create_container()
            
        # Upload the JSON string
        blob_client = container_client.get_blob_client(filename)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        logging.info(f"Saved {filename} to container '{container_name}'.")

        # 3. TRANSFORM (Pandas)
        # Flatten the JSON. Adjust this line to match YOUR specific API structure.
        # For CoinDesk API, the useful data is nested under 'bpi'
        df = pd.json_normalize(data)

        # 4. INSERT TO SQL (Silver/Gold Layer)
        logging.info("Connecting to SQL Database...")
        with pyodbc.connect(sql_conn_str) as conn:
            cursor = conn.cursor()
            
            # Create Table if it doesn't exist (Idempotency)
            # Warning: In production, do this via a separate migration script, not here.
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CryptoPrices' AND xtype='U')
                CREATE TABLE CryptoPrices (
                    UpdatedTime NVARCHAR(100),
                    Price float,
                    IngestionDate DATETIME DEFAULT GETDATE()
                )
            """)

            # Insert Data
            # Note: Adjust row['...'] keys to match your API's JSON structure
            for index, row in df.iterrows():
                cursor.execute(
                    "INSERT INTO CryptoPrices (UpdatedTime, Price) VALUES (?, ?)",
                    row['time.updated'], 
                    row['bpi.USD.rate_float']
                )
            
            conn.commit()
            logging.info("Data inserted into SQL successfully!")

    except Exception as e:
        logging.error(f"PIPELINE FAILED: {str(e)}")
        raise e