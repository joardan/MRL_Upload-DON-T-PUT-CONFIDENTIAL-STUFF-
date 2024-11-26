import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class LogHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()

    def on_modified(self, event):
        if event.src_path.endswith("analysis.log"):
            print("analysis.log was modified")
            with open(event.src_path, 'r') as f:
                lines = f.readlines()
                if lines and "*************************************************" in lines[-3]:
                    print("Detected line in analysis.log, initiating CSV upload")
                    self.csv_folder = os.path.dirname(event.src_path)
                    self.upload_csv_files()

    def upload_csv_files(self):
        csv_files = [f for f in os.listdir(self.csv_folder) if f.endswith('.csv') and f.startswith('AXLTBL')]
        if not csv_files:
            print(f"No AXLTBL CSV files found in {self.csv_folder}")
            return

        for csv_file in csv_files:
            file_path = os.path.join(self.csv_folder, csv_file)
            self.upload_file(file_path)

    def upload_file(self, file_path):
        # Implement your upload logic here
        print(f"Test Uploading file: {file_path}")
        # Example: Move file to 'uploaded' folder (replace with actual upload logic)

if __name__ == "__main__":
    path = r"D:\Server\trains"

    event_handler = LogHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    
    print(f"Starting to watch file: {path}")
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()