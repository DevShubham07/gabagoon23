import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

def download_from_r2():
    # R2 configuration
    account_id = os.getenv('R2_ACCOUNT_ID')
    access_key_id = os.getenv('R2_ACCESS_KEY_ID')
    secret_access_key = os.getenv('R2_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('R2_BUCKET_NAME')
    
    if not all([account_id, access_key_id, secret_access_key, bucket_name]):
        print("Error: Missing environment variables in env_file.txt.")
        return

    # S3 client for Cloudflare R2
    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=Config(signature_version='s3v4'),
        region_name='auto'
    )

    print(f"Listing objects in bucket '{bucket_name}'...")
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        # We start from the root of the bucket
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' not in page:
                print("Bucket is empty.")
                return

            for obj in page['Contents']:
                key = obj['Key']
                # Skip folders (indicated by keys ending in /)
                if key.endswith('/'):
                    continue
                
                # Local path construction (save everything under the root dir)
                local_file_path = Path(key)
                
                # Create directories if needed
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                print(f"Downloading: {key}...", end="\r")
                s3.download_file(bucket_name, key, str(local_file_path))
                
        print("\nDownload complete! All files saved in current directory.")
        
    except Exception as e:
        print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    download_from_r2()
