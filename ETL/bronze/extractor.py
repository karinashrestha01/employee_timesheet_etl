from pathlib import Path

from minio import Minio

import sys, os
from dotenv import load_dotenv

import logging
logger = logging.getLogger(__name__)

def extract_from_minio(
    *,
    secure: bool = False
) -> str:
    """
    Extract data files from MinIO object storage.
    
    Args:
        endpoint: MinIO server endpoint
        access_key: MinIO access key
        secret_key: MinIO secret key
        bucket_name: Bucket containing source files
        download_dir: Local directory to save files
        secure: Use HTTPS connection
    
    Returns:
        Path to download directory
    
    Raises:
        ExtractionError: If extraction fails
    """
    """Download the raw files from minIO bucket."""
    # endpoint = bronze_settings.minio_host
    # access_key = bronze_settings.minio_root_user
    # secret_key = bronze_settings.minio_root_password
    # bucket_name = bronze_settings.minio_bucket_name
    # download_dir_str = bronze_settings.minio_download_dir

    endpoint: str = os.getenv("MINIO_HOST")
    access_key: str  = os.getenv("MINIO_ROOT_USER")
    secret_key: str  = os.getenv("MINIO_ROOT_PASSWORD")
    bucket_name: str  = os.getenv("MINIO_BUCKET_NAME")
    download_dir_str: str  = os.getenv("MINIO_DOWNLOAD_DIR")

    if not all([endpoint, access_key, secret_key, bucket_name, download_dir_str]):
        msg = "Missing MinIO Configuration"
        raise ValueError(msg)

    download_path = Path(download_dir_str)

    logger.info("Connecting to MinIO at: %s", endpoint)
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    download_path.mkdir(parents=True, exist_ok=True)

    objects = client.list_objects(bucket_name, recursive=True)
    downloaded_files: list[Path] = []

    for obj in objects:
        local_file_path = download_path / obj.object_name
        local_file_path.parent.mkdir(parents=True, exist_ok=True)
        client.fget_object(bucket_name, obj.object_name, str(local_file_path))
        downloaded_files.append(local_file_path)
        logger.info("Downloaded: %s", local_file_path)

    logger.info("Download complete: %s files downloaded", len(downloaded_files))
    return str(download_path)


if __name__ == "__main__":
    try:
        download_path = extract_from_minio()
        logger.info("Files downloaded to: %s", download_path)
    except Exception:
        logger.exception("Download failed due to an unexpected error")

