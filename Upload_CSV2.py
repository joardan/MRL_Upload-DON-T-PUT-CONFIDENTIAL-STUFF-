import os
import boto3
import argparse

# S3 bucket configuration
S3_BUCKET_NAME = "wabtec-bucket"
S3_REGION_NAME = "ap-southeast-2"
LOCAL_STORAGE_PATH = r"D:\Server\trains"

def upload_axltbl_csvs(local_path, bucket_name, region_name, args):
    """
    Upload only CSV files starting with 'AXLTBL' to an S3 bucket,
    formatted in year/month/day/hour structure, from May 2024 onwards.
    """
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=args.aws_access_key,
        aws_secret_access_key=args.aws_secret_key,
        aws_session_token=args.aws_session_token,
        region_name=region_name
    )

    for root, dirs, files in os.walk(local_path):
        for file in files:
            # Only process files that are CSV and start with 'AXLTBL'
            if file.endswith(".csv") and file.startswith("AXLTBL"):
                file_path = os.path.join(root, file)
                
                # Extract the relative path (e.g., '2024/1121/200123')
                relative_path = os.path.relpath(file_path, LOCAL_STORAGE_PATH)
                parts = relative_path.split(os.sep)

                if len(parts) >= 3:  # Ensure it has at least year/monthday/hourminute_seconds
                    year = int(parts[0])
                    monthday = parts[1]
                    month = int(monthday[:2])
                    day = int(monthday[2:])
                    hourminute_seconds = parts[2]

                    # Check if the year and month meet the criteria
                    if (year > 2024) or (year == 2024 and month >= 5):
                        # Construct S3 key in "year/month/day/hour/filename" format
                        s3_key = f"wabtecdata/{year}/{month:02d}/{day:02d}/{hourminute_seconds}/{file}"
                        
                        print(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}...")
                        try:
                            s3_client.upload_file(file_path, bucket_name, s3_key)
                            print(f"Successfully uploaded: {s3_key}")
                        except Exception as e:
                            print(f"Failed to upload {file_path}: {e}")
                    else:
                        print(f"Skipping file from before May 2024: {relative_path}")
                else:
                    print(f"Skipping invalid path structure: {relative_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload new CSV files to S3.")
    parser.add_argument("aws_access_key", help="AWS access key ID")
    parser.add_argument("aws_secret_key", help="AWS secret access key")
    parser.add_argument("aws_session_token", help="AWS session token")

    # Check that the path exists
    if not os.path.exists(LOCAL_STORAGE_PATH):
        print(f"Local storage path does not exist: {LOCAL_STORAGE_PATH}")
    else:
        args = parser.parse_args()
        upload_axltbl_csvs(LOCAL_STORAGE_PATH, S3_BUCKET_NAME, S3_REGION_NAME, args)