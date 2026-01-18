import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv
from pathlib import Path

    # Load environment variables
    load_dotenv()

def upload_to_r2(local_directory):
    # R2 configuration
    account_id = os.getenv('R2_ACCOUNT_ID')
    access_key_id = os.getenv('R2_ACCESS_KEY_ID')
    secret_access_key = os.getenv('R2_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('R2_BUCKET_NAME')
    
    if not all([account_id, access_key_id, secret_access_key, bucket_name]):
        print("Error: Missing environment variables. Please check your .env file.")
        return

    # S3 client for Cloudflare R2
    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=Config(signature_version='s3v4'),
        region_name='auto'  # R2 expects region to be 'auto'
    )

    local_path = Path(local_directory)
    if not local_path.exists():
        print(f"Error: Local directory '{local_directory}' does not exist.")
        return

    print(f"Starting upload from '{local_directory}' to bucket '{bucket_name}' root...")

    # Iterate through all files in the directory recursively
    for file_path in local_path.rglob('*'):
        if file_path.is_file():
            # Calculate the relative path from the base directory
            # For "saving it all under the root dir", we use the relative path as the key
            # If the user wants the structure preserved, we use relative_to(local_path)
            # If they want everything flattened to root, that would be risky (name collisions)
            # Usually "save it under root" means the contents of the folder should be at root
            relative_key = str(file_path.relative_to(local_path))
            
            try:
                print(f"Uploading: {relative_key}...", end="\r")
                s3.upload_file(
                    Filename=str(file_path),
                    Bucket=bucket_name,
                    Key=relative_key
                )
            except Exception as e:
                print(f"\nFailed to upload {relative_key}: {e}")

    print("\nUpload complete!")

if __name__ == "__main__":
    # The user mentioned the directory structure: polymarket/data/2025/12/
    # We'll look for this directory locally.
    target_dir = "polymarket/data/2025/12"
    
    if not os.path.exists(target_dir):
        # Try to find it if it's somewhere else or if they are running from root
        print(f"Directory '{target_dir}' not found in current path.")
        user_input = input("Please enter the local path to the directory (or press Enter to exit): ")
        if user_input:
            target_dir = user_input
        else:
            exit()

    upload_to_r2(target_dir)
