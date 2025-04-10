import boto3
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

def list_prefixes(bucket_name, delimiter='/'):
    """List all prefixes in the given S3 bucket."""
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    prefixes = set()

    for page in paginator.paginate(Bucket=bucket_name, Delimiter=delimiter):
        for prefix in page.get('CommonPrefixes', []):
            prefixes.add(prefix['Prefix'])

    return list(prefixes)

def run_source_transfer(machine, bucket_name, prefix, archive_file, destination_host, destination_path):
    """Run source_transfer.py on a remote machine."""
    env_vars = {
        "SOURCE_BUCKET": bucket_name,
        "SOURCE_PREFIX": prefix,
        "ARCHIVE_FILE": archive_file,
        "DESTINATION_HOST": destination_host,
        "DESTINATION_PATH": destination_path,
    }
    env_string = " ".join([f"{key}={value}" for key, value in env_vars.items()])
    command = f"ssh {machine} '{env_string} python3 /home/opc/disbatch/source_transfer.py'"
    subprocess.run(command, shell=True, check=True)

if __name__ == "__main__":
    bucket_name = os.getenv("SOURCE_BUCKET")
    destination_host = os.getenv("DESTINATION_HOST")
    destination_path = os.getenv("DESTINATION_PATH")
    machines = os.getenv("MACHINES").split(",")  # Comma-separated list of machine hostnames
    archive_file_template = os.getenv("ARCHIVE_FILE_TEMPLATE", "archive_{prefix}.tar.xz")

    # Get the list of prefixes
    prefixes = list_prefixes(bucket_name)

    # Distribute tasks across machines
    with ThreadPoolExecutor() as executor:
        futures = []
        for i, prefix in enumerate(prefixes):
            machine = machines[i % len(machines)]  # Round-robin assignment
            archive_file = archive_file_template.format(prefix=prefix.replace("/", "_"))
            futures.append(
                executor.submit(
                    run_source_transfer,
                    machine,
                    bucket_name,
                    prefix,
                    archive_file,
                    destination_host,
                    destination_path,
                )
            )

        # Wait for all tasks to complete
        for future in futures:
            future.result()
