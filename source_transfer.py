import boto3
import os
import subprocess
import time
import random
from concurrent.futures import ThreadPoolExecutor

def download_file(s3, bucket_name, file_key, local_path, max_retries=5):
    attempt = 0
    while attempt < max_retries:
        try:
            s3.download_file(bucket_name, file_key, local_path)
            return  # Exit if successful
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                raise e  # Re-raise the exception if max retries reached
            wait_jitter = 2 + random.random()  # Add jitter to avoid synchronized retries
            wait_time = wait_jitter ** attempt  # Exponential backoff
            time.sleep(wait_time)

def compress_files(bucket_name, prefix, output_file):
    s3 = boto3.client('s3')
    os.makedirs('temp', exist_ok=True)
    paginator = s3.get_paginator('list_objects_v2')
    file_keys = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            file_keys.append(obj['Key'])

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                download_file,
                s3,
                bucket_name,
                file_key,
                os.path.join('temp', os.path.basename(file_key))
            )
            for file_key in file_keys
        ]
        for future in futures:
            future.result()  # Wait for all downloads to complete

    # Use partar to create the tarball in parallel
    subprocess.run(['partar', 'czf', '-P ',threads, output_file, '-C', 'temp', '.'], check=True)

    # Clean up temporary files
    for root, _, files in os.walk('temp'):
        for file in files:
            os.remove(os.path.join(root, file))
    os.rmdir('temp')

def transfer_archive(archive_path, destination_host, destination_path):
    subprocess.run(['scp', archive_path, f'{destination_host}:{destination_path}'], check=True)

if __name__ == "__main__":
    bucket_name = os.getenv("SOURCE_BUCKET")
    prefix = os.getenv("SOURCE_PREFIX")
    output_file = os.getenv("ARCHIVE_FILE", "archive.tar.gz")  # Update default extension
    destination_host = os.getenv("DESTINATION_HOST")
    destination_path = os.getenv("DESTINATION_PATH")
    threads = 2  # Number of threads for parallel compression

    compress_files(bucket_name, prefix, output_file, threads)
    transfer_archive(output_file, destination_host, destination_path)
    os.remove(output_file)
