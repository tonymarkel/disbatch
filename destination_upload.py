import oci
import tarfile
import os
import random
import time

def decompress_archive(archive_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    with tarfile.open(archive_path, 'r:gz') as tar:
        tar.extractall(path=output_dir)

def upload_to_oci(bucket_name, namespace, directory, max_retries=5):
    config = oci.config.from_file()
    object_storage = oci.object_storage.ObjectStorageClient(config)
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            object_name = os.path.relpath(file_path, directory)
            
            # Check if the object already exists with retries
            head_attempt = 0
            while head_attempt < max_retries:
                try:
                    object_storage.head_object(namespace, bucket_name, object_name)
                    continue  # Skip upload if the object exists
                except oci.exceptions.ServiceError as e:
                    if e.status != 404:
                        head_attempt += 1
                        if head_attempt >= max_retries:
                            raise e  # Re-raise the exception if max retries reached
                        # Exponential backoff with jitter
                        base_wait = 2 ** head_attempt  # Exponential growth
                        jitter = random.uniform(0, 1)  # Add randomness
                        wait_time = base_wait + jitter
                        time.sleep(wait_time)
                    else:
                        break  # Exit loop if it's a "Not Found" error
            
            # Upload the object with retries
            put_attempt = 0
            while put_attempt < max_retries:
                try:
                    with open(file_path, 'rb') as f:
                        object_storage.put_object(namespace, bucket_name, object_name, f)
                    break  # Exit loop if successful
                except Exception as e:
                    put_attempt += 1
                    if put_attempt >= max_retries:
                        raise e  # Re-raise the exception if max retries reached
                    # Exponential backoff with jitter
                    base_wait = 2 ** put_attempt  # Exponential growth
                    jitter = random.uniform(0, 1)  # Add randomness
                    wait_time = base_wait + jitter
                    time.sleep(wait_time)

if __name__ == "__main__":
    archive_path = os.getenv("ARCHIVE_PATH")
    output_dir = os.getenv("OUTPUT_DIR", "decompressed_files")
    bucket_name = os.getenv("DESTINATION_BUCKET")
    namespace = os.getenv("OCI_NAMESPACE")

    decompress_archive(archive_path, output_dir)
    upload_to_oci(bucket_name, namespace, output_dir)

    # Clean up decompressed files
    for root, _, files in os.walk(output_dir):
        for file in files:
            os.remove(os.path.join(root, file))
    os.rmdir(output_dir)
