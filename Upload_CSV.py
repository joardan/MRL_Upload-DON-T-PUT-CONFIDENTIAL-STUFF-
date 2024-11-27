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
    formatted in year/month/day/hour structure.
    """
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=args.aws_access_key,
        aws_secret_access_key=args.aws_secret_key,
        region_name=region_name
    )

    for root, dirs, files in os.walk(local_path):
        for file in files:
            # Only process files that are CSV and start with 'AXLTBL'
            if file.endswith(".csv") and file.startswith("AXLTBL"):
                file_path = os.path.join(root, file)
                
                file_name = os.path.basename(file_path)
                name_parts = file_name.split('_')
                
                if len(name_parts) != 3:
                    print(f"Skipping invalid file name structure: {file_name}")
                    continue
                
                try:
                    date_part = name_parts[1]
                    time_part = name_parts[2].replace('.csv', '')

                    year, month, day = date_part.split('-')
                    hour, _ = time_part.split('-')

                    # Construct S3 key in "year/month/day/hour/filename" format, ignoring minutes
                    s3_key = f"wabtec_data/{year}/{month}/{day}/{hour}/{file}"

                    print(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}...")
                    s3_client.upload_file(file_path, bucket_name, s3_key)
                    print(f"Successfully uploaded: {s3_key}")

                except Exception as e:
                    print(f"Failed to upload {file_path}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload new CSV files to S3.")
    parser.add_argument("aws_access_key", help="AWS access key ID")
    parser.add_argument("aws_secret_key", help="AWS secret access key")

    args = parser.parse_args()

    # Check that the path exists
    if not os.path.exists(LOCAL_STORAGE_PATH):
        print(f"Local storage path does not exist: {LOCAL_STORAGE_PATH}")
    else:
        upload_axltbl_csvs(LOCAL_STORAGE_PATH, S3_BUCKET_NAME, S3_REGION_NAME, args)