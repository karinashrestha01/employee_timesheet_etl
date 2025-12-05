import os
import pandas as pd
from minio import Minio

class EmployeeETL:
    """Complete ETL Pipeline for Employee Data from MinIO"""
    
    def __init__(self, minio_config=None):
        """
        Initialize ETL Pipeline
        
        Args:
            minio_config: Dict with MinIO connection details
                {
                    'endpoint': 'localhost:9000',
                    'access_key': 'admin',
                    'secret_key': 'password',
                    'bucket_name': 'rawdata',
                    'secure': False
                }
        """
        self.minio_config = minio_config or {
            'endpoint': 'localhost:9000',
            'access_key': 'admin',
            'secret_key': 'password',
            'bucket_name': 'rawdata',
            'secure': False
        }
        self.raw_df = None
        self.download_dir = "datafile"
        
    # ==================== EXTRACT ====================
    
    def extract_from_minio(self):
        """Extract CSV files from MinIO bucket"""
        try:
            # Connect to MinIO
            client = Minio(
                self.minio_config['endpoint'],
                access_key=self.minio_config['access_key'],
                secret_key=self.minio_config['secret_key'],
                secure=self.minio_config['secure']
            )
            
            bucket_name = self.minio_config['bucket_name']
            os.makedirs(self.download_dir, exist_ok=True)
            
            # List and download all objects
            objects = client.list_objects(bucket_name, recursive=True)
            downloaded_files = []
            
            for obj in objects:
                local_path = os.path.join(self.download_dir, obj.object_name)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                client.fget_object(bucket_name, obj.object_name, local_path)
                downloaded_files.append(local_path)
                print(f"Downloaded: {local_path}")
            
            return downloaded_files
            
        except Exception as e:
            print(f" MinIO extraction failed: {e}")
            raise
    
    def load_csv(self, file_path):
        """Load CSV file with proper handling of pipe delimiter"""
        try:
            self.raw_df = pd.read_csv(
                file_path,
                delimiter='|',
                quotechar='"',
                skipinitialspace=True,
                na_values=['', 'NULL', 'null', 'nan'],
                keep_default_na=True
            )
            
            # Strip whitespace from column names
            self.raw_df.columns = self.raw_df.columns.str.strip()
            
            print(f"Loaded {len(self.raw_df)} records from {file_path}")
            print(f"Columns: {list(self.raw_df.columns)}")
            return self.raw_df
            
        except Exception as e:
            print(f" CSV loading failed: {e}")
            raise