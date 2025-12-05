import os
from minio import Minio

def extract_from_minio():

    # Step 1: Connect to MinIO
    client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password",
        secure=False
    )

    bucket_name = "rawdata"    # change if your bucket name is different
    download_dir = "datasets"  # local folder to save data

    os.makedirs(download_dir, exist_ok=True)
    objects = client.list_objects(bucket_name, recursive=True)

    #if the data is huge we can either do batch processing or partition
    for obj in objects:

        # Create local file path
        local_path = os.path.join(download_dir, obj.object_name)

        # Create folders if they don't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Download file
        client.fget_object(bucket_name, obj.object_name, local_path)

        print(f"Downloaded: {local_path}")

    return download_dir

if __name__ == "__main__":
    extract_from_minio()
