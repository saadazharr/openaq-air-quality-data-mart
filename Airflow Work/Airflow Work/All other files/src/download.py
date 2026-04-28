import os    
import boto3 
import logging

logging.basicConfig( 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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