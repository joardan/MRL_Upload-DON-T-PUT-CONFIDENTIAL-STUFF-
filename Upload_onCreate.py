import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FolderHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self.last_folder = None
        self.waiting_for_next = False

    def on_created(self, event):
        if not event.is_directory:
            return

        folder_path = event.src_path
        folder_name = os.path.basename(folder_path)
        parent_folder = os.path.basename(os.path.dirname(folder_path))
        
        # Check if this is a "MMDD" folder (day folder) under a year folder
        if len(folder_name) == 4 and folder_name.isdigit() and parent_folder.isdigit() and len(parent_folder) == 4:
            print(f"New day folder detected: {folder_path}")

            if self.waiting_for_next:
                print(f"Subsequent day folder detected: {folder_path}")
                self.waiting_for_next = False
                self.upload_last_folder()
            
            self.last_folder = folder_path
            self.waiting_for_next = True

        # Check if this is a "YYYY" folder (year folder)
        elif len(folder_name) == 4 and folder_name.isdigit() and (folder_name.startswith("19") or folder_name.startswith("20")):
            print(f"New year folder detected: {folder_path}")

    def upload_last_folder(self):
        if not self.last_folder:
            return
        
        csv_files = [f for f in os.listdir(self.last_folder) if f.endswith('.csv')]
        if not csv_files:
            print(f"No CSV files found in {self.last_folder}")
            return
        
        for csv_file in csv_files:
            file_path = os.path.join(self.last_folder, csv_file)
            self.upload_file(file_path)

    def upload_file(self, file_path):
        # Implement your upload logic here
        print(f"Test Uploading file: {file_path}")
        # Example: Move file to 'uploaded' folder (replace with actual upload logic)

if __name__ == "__main__":
    path = r"D:\Server\trains"  # Change this to your top-level directory
    event_handler = FolderHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)

    print(f"Starting to watch directory: {path}")
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()