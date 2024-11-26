import os
import time
import boto3
import argparse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# S3 bucket configuration
S3_BUCKET_NAME = "wabtec-bucket"
S3_REGION_NAME = "ap-southeast-2"
LOCAL_STORAGE_PATH = r"D:\Server\trains"

class LogHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self.last_processed_folder = None
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            aws_session_token=aws_session_token,
            region_name=S3_REGION_NAME
        )

    def on_modified(self, event):
        if event.src_path.endswith("analysis.log"):
            current_folder = os.path.dirname(event.src_path)

            # If current folder is the same as last processed folder, skip processing
            if current_folder == self.last_processed_folder:
                print(f"Modification detected in {current_folder}, but it's already processed.")
                return

            with open(event.src_path, 'r') as f:
                lines = f.readlines()
                if len(lines) >= 2 and "*************************************************" in lines[-2]:
                    print("Detected line in analysis.log, initiating CSV upload")
                    self.csv_folder = current_folder
                    self.upload_csv_files()
                    self.last_processed_folder = current_folder

    def upload_csv_files(self):
        csv_files = [f for f in os.listdir(self.csv_folder) if f.endswith('.csv') and f.startswith('AXLTBL')]
        if not csv_files:
            print(f"No AXLTBL CSV files found in {self.csv_folder}")
            return

        for csv_file in csv_files:
            file_path = os.path.join(self.csv_folder, csv_file)
            self.upload_file(file_path)

    def upload_file(self, file_path):
        relative_path = os.path.relpath(file_path, LOCAL_STORAGE_PATH)
        parts = relative_path.split(os.sep)

        if len(parts) >= 3:  # Ensure it has at least year/monthday/hourminute_seconds
            year = parts[0]
            monthday = parts[1]
            hourminute_seconds = parts[2]

            # Reformat month/day/hour structure
            month = monthday[:2]
            day = monthday[2:]

            # Construct S3 key in "wabtecdata/year/month/day/hour/filename" format
            s3_key = f"wabtecdata/{year}/{month}/{day}/{hourminute_seconds}/{os.path.basename(file_path)}"

            print(f"Uploading {file_path} to s3://{S3_BUCKET_NAME}/{s3_key}...")
            try:
                self.s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_key)
                print(f"Successfully uploaded: {s3_key}")
            except Exception as e:
                print(f"Failed to upload {file_path}: {e}")
        else:
            print(f"Skipping invalid path structure: {relative_path}")

if __name__ == "__main__":
    # Check that the path exists
    if not os.path.exists(LOCAL_STORAGE_PATH):
        print(f"Local storage path does not exist: {LOCAL_STORAGE_PATH}")
    else:
        event_handler = LogHandler()
        observer = Observer()
        observer.schedule(event_handler, LOCAL_STORAGE_PATH, recursive=True)

        print(f"Starting to watch file: {LOCAL_STORAGE_PATH}")
        observer.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()