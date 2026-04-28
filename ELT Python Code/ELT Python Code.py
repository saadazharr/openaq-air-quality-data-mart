import os    
import boto3 
import pandas as pd
import pyodbc
import math
import logging

logging.basicConfig(
    filename='data_processing.log', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DB:
    def __init__(self, conn_str):
        self.conn_str = conn_str
        self.conn = None
        self.cursor = None

    def get_session(self):
        self.conn = pyodbc.connect(self.conn_str)
        self.cursor = self.conn.cursor()
        logging.info("Database connection established.")

    def close_session(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("Database connection closed.")

def download_files_from_s3(bucket_name, download_path):
    s3 = boto3.client('s3')

    if not os.path.exists(download_path):
        os.makedirs(download_path)
        logging.info(f"Created directory: {download_path}")

    response = s3.list_objects_v2(Bucket=bucket_name)

    if 'Contents' in response:
        for item in response['Contents']:
            file_key = item['Key']
            if file_key.endswith('.csv'):
                download_file_path = os.path.join(download_path, file_key)
                try:
                    s3.download_file(bucket_name, file_key, download_file_path)
                    logging.info(f"{file_key} downloaded successfully to {download_file_path}.")
                except Exception as e:
                    logging.error(f"Error downloading {file_key}: {e}")
    else:
        logging.warning("No files found in the S3 bucket.")
    return download_path

def import_to_sql(csv_folder_path, table_name, db_instance):
    for file_name in os.listdir(csv_folder_path):
        if file_name.endswith('.csv'):
            csv_path = os.path.join(csv_folder_path, file_name)
            try:
                df = pd.read_csv(csv_path)
                logging.info(f"Processing file: {file_name}")

                df = df.replace(r'^\s*$', None, regex=True)
                df = df.where(pd.notnull(df), None)

                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
                df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

                for index, row in df.iterrows():
                    try:
                        row = [None if (isinstance(x, float) and math.isnan(x)) else x for x in row]
                        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['?'] * len(df.columns))})"
                        db_instance.cursor.execute(insert_query, row)
                    except Exception as e:
                        logging.error(f"Error inserting row {index} from file {file_name}: {e}")
                        continue

                db_instance.conn.commit()
                logging.info(f"Data from {file_name} loaded successfully.")
            except Exception as e:
                logging.error(f"Error processing file {file_name}: {e}")

if __name__ == "__main__":
    bucket_name = "openaq-data"
    download_path = r"D:\Personal\SQL\Files for Loading Data in the database"
    csv_folder_path = r"D:\Personal\SQL\Files for Loading Data in the database"

    download_files_from_s3(bucket_name, download_path)

    conn_str = (
        "Driver={SQL Server};"
        "Server=DESKTOP-LLJHUN0;"
        "Database=AirQualityData;"
        "Trusted_Connection=yes;"
    )

    db_instance = DB(conn_str)
    db_instance.get_session()

    table_name = "all_csv_files"
    import_to_sql(csv_folder_path, table_name, db_instance)

    db_instance.close_session()
