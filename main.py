import boto3
import os
import botocore
import json
s3 = boto3.client('s3')

def create_bucket(bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})
        print(f"Bucket - {bucket_name} is created successfully!")
    except botocore.exceptions.ClientError as e:
        print(f"Error: {e}")

def list_buckets():
    try:
        response = s3.list_buckets()
        buckets = response["Buckets"]
        if buckets:
            print("S3 Buckets:")
            for bucket in buckets:
                print(f"- {bucket['Name']} (Created: {bucket['CreationDate']})")
            return buckets
        else:
            print("No S3 buckets found.")
    except botocore.exceptions.ClientError as e:
        print(f"Error: {e}")

def upload_object(file_path,bucket_name,folder_name):
    try:
        if not folder_name.endswith('/'):
            folder_name += '/'
        file_name = file_path.split("/")[-1]  # Extract file name from path
        s3_key = folder_name + file_name  # S3 path including folder
        #s3_key - the file name store in bucket
        # upload_file_name = os.path.basename(file_path)
        with open(file_path, "r"):
            s3.upload_file(file_path, bucket_name, s3_key)
            print(f"File - {file_name} uploaded successfully to {bucket_name}/{s3_key}.")
    except botocore.exceptions.ClientError as e:
        print(f"Error: {e}")

def create_folder(bucket_name,folders):
    try:
        for folder in folders:
            print(f"Creating {folder} in {bucket_name} bucket...")
            s3.put_object(Bucket=bucket_name, Key=folder)
            print(f"{folder} created in {bucket_name} successfully.")
    except Exception as e:
        print(f"Error: {e}")

def upload_directory(local_directory, bucket_name, s3_prefix):
    try:
        for root, dirs, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)  # Full local file path
                relative_path = os.path.relpath(local_path, local_directory)  # Relative path from the base directory
                s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")  # Convert to S3 format

                print(f"Uploading {local_path} to s3://{bucket_name}/{s3_path}")
                s3.upload_file(local_path, bucket_name, s3_path)
        print(f"Total No.of {len(files)} files uploaded.")
    except Exception as e:
        print(f"Error: {e}")

def download_object(bucket_name,upload_file_name):
    try:
        s3.download_file(bucket_name, upload_file_name, 'downloaded-1.txt')
        print(f"File {upload_file_name} downloaded successfully from s3 bucket {bucket_name}.")
    except botocore.exceptions.ClientError as e:
        print(f"Error: {e}")

def list_objects(bucket_name,prefix):
    try:
        response = s3.list_objects_v2(Bucket = bucket_name, Prefix=prefix)
        if 'Contents' in response:
            print(f"Objects in bucket '{bucket_name}':")
            for obj in response['Contents']:
                print(f"- {obj['Key']} (Last modified: {obj['LastModified']}, Size: {obj['Size']} bytes)")
        else:
            print(f"No objects found in bucket '{bucket_name}'.")
    except botocore.exceptions.ClientError as e:
        print(f"Error: {e}")

def read_object(bucket_name, file_name):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        json_data = response['Body'].read().decode('utf-8')
        data = json.loads(json_data)
        print(data)
    except botocore.exceptions.ClientError as e:
        print(f"Error: {e}")

def delete_object(bucket_name,upload_file_name):
    try:
        if s3.head_object(Bucket=bucket_name, Key=upload_file_name):
            s3.delete_object(Bucket=bucket_name, Key=upload_file_name)
            print(f"Object '{upload_file_name}' deleted successfully!")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"File '{upload_file_name}' not found in bucket '{bucket_name}'.")
        else:
            print(f"Error: {e}")

def delete_bucket(bucket_name):
    try:
        if s3.head_bucket(Bucket=bucket_name):
            print(f"{bucket_name} exists!")
            # List and delete all objects in the bucket (if any)
            response = s3.list_objects_v2(Bucket=bucket_name)
            if "Contents" in response:
                for obj in response["Contents"]:
                    print(f"Deleting object: {obj['Key']}")
                    s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
            # After emptying the bucket, delete the bucket itself
            s3.delete_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' deleted successfully!")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"{bucket_name} not found.")
        else:
            print(f"Error checking bucket: {e}")

# create_bucket('test-gautham-s3')
# list_buckets()
# list_objects('ivgautham-s3','test5/')
read_object('football-analysis-s3','competitions.json')
# upload_object('D:/Gowtham/learnings/my_project/football_analysis/data/data/three-sixty/','football-analysis-s3','data/three-sixty/')
# download_object('ivgautham-s3','README.txt')
# delete_object('test-football-analysis','README.txt')
# delete_bucket('test-gautham-s3')

# folders = ['three-sixty/']
# create_folder('football-analysis-s3',folders)

# local_dir = r"D:\Gowtham\learnings\my_project\football_analysis\data\data\three-sixty_s3"
# upload_directory(local_dir,'football-analysis-s3','three-sixty/')