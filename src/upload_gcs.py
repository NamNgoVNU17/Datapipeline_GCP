from google.cloud import storage
import sys
import os
sys.path.insert(0, os.path.abspath("/opt/airflow"))
from src.utils.logger import setup_logger
from tenacity import retry, stop_after_attempt, wait_fixed
import os

logger = setup_logger(__name__)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
def upload_to_gcs(file_path: str, bucket_name: str, blob_path: str):
    if not os.path.exists(file_path) :
        logger.error(f"File {file_path} khong ton tai")
        return
    try :
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(file_path)
        logger.info(f"Đã upload {file_path} lên gs://{bucket_name}/{blob_path}")

        if blob.exists(client) :
            logger.info("Upload thành công")
        else :
            logger.warning("Không xác nhận được upload")
    except Exception as e :
        logger.exception(f"Upload thất bại: {e}")
        raise
