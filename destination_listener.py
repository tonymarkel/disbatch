import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

def run_destination_upload(archive_path, bucket_name, namespace):
    """Run destination_upload.py with the given parameters."""
    env_vars = {
        "ARCHIVE_PATH": archive_path,
        "DESTINATION_BUCKET": bucket_name,
        "OCI_NAMESPACE": namespace,
    }
    env_string = " ".join([f"{key}={value}" for key, value in env_vars.items()])
    command = f"{env_string} python3 /home/opc/disbatch/destination_upload.py"
    subprocess.run(command, shell=True, check=True)

if __name__ == "__main__":
    # Environment variables
    bucket_name = os.getenv("DESTINATION_BUCKET")
    namespace = os.getenv("OCI_NAMESPACE")
    archive_dir = os.getenv("ARCHIVE_DIR", "/home/opc/archives")  # Directory to monitor for archives

    # Ensure the archive directory exists
    os.makedirs(archive_dir, exist_ok=True)

    # Monitor the directory for new archive files
    with ThreadPoolExecutor() as executor:
        processed_files = set()
        while True:
            archive_files = {
                os.path.join(archive_dir, f)
                for f in os.listdir(archive_dir)
                if f.endswith(".tar.xz")
            }
            new_files = archive_files - processed_files
            for archive_path in new_files:
                executor.submit(run_destination_upload, archive_path, bucket_name, namespace)
                processed_files.add(archive_path)
