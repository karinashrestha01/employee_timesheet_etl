import os
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)


def extract_from_minio(
    endpoint: str = "minio:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    bucket_name: str = "rawdata",
    download_dir: str = "datasets",
    secure: bool = False,
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
    try:
        from minio import Minio
    except ImportError:
        logger.error("minio package not installed. Run: pip install minio")
        raise ImportError("minio package required for MinIO extraction")
    
    logger.info(f"Connecting to MinIO at {endpoint}")
    
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    
    os.makedirs(download_dir, exist_ok=True)
    
    objects = client.list_objects(bucket_name, recursive=True)
    downloaded_files: List[str] = []
    
    for obj in objects:
        # Create local file path
        local_path = os.path.join(download_dir, obj.object_name)
        
        # Create folders if they don't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download file
        client.fget_object(bucket_name, obj.object_name, local_path)
        downloaded_files.append(local_path)
        logger.info(f"Downloaded: {local_path}")
    
    logger.info(f"Extraction complete: {len(downloaded_files)} files downloaded")
    return download_dir


# def extract_from_local(
#     source_dir: str,
#     download_dir: str = "datasets",
#     file_patterns: Optional[List[str]] = None,
# ) -> str:
#     import shutil
#     from pathlib import Path
    
#     source = Path(source_dir)
#     target = Path(download_dir)
#     target.mkdir(parents=True, exist_ok=True)
    
#     patterns = file_patterns or ["*.csv"]
#     copied_files = []
    
#     for pattern in patterns:
#         for file_path in source.glob(pattern):
#             dest_path = target / file_path.name
#             shutil.copy2(file_path, dest_path)
#             copied_files.append(str(dest_path))
#             logger.info(f"Copied: {file_path.name}")
    
#     logger.info(f"Local extraction complete: {len(copied_files)} files copied")
#     return download_dir


if __name__ == "__main__":
    extract_from_minio()
