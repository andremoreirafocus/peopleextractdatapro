
import requests
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import json
from datetime import datetime
import time

def write_data_to_minio(data, bucket_name, object_name, minio_endpoint, access_key, secret_key, secure=True):
    """
    Uploads a variable (Python object) as a JSON file to a MinIO bucket.
    :param data: Python object to upload (will be serialized to JSON)
    :param bucket_name: Name of the MinIO bucket
    :param object_name: Name for the object in the bucket
    :param minio_endpoint: MinIO server endpoint (e.g., 'localhost:9000')
    :param access_key: MinIO access key
    :param secret_key: MinIO secret key
    :param secure: Use HTTPS if True, HTTP if False
    """
    client = Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )
    try:
        # Create bucket if it does not exist
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        # Serialize data to JSON and upload
        json_bytes = json.dumps(data).encode('utf-8')
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=BytesIO(json_bytes),
            length=len(json_bytes),
            content_type='application/json'
        )
        print(f"Data uploaded to bucket '{bucket_name}' as '{object_name}'")
    except S3Error as err:
        print(f"MinIO error: {err}")


def get_json_response(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
    except ValueError as json_err:
        print(f"JSON decode error: {json_err}")
    return None

def extract_and_load(api_server, bucket_name, object_name, minio_endpoint, access_key, secret_key, secure=True):
    print("extracting data from API server")
    data = get_json_response(api_server)
    if data:
        print(f"Data extracted {data}")
        write_data_to_minio(data, bucket_name, object_name, minio_endpoint, access_key, secret_key, secure)
        
def main():
    api_server = "https://randomuser.me/api/"
    minio_endpoint = "localhost:9000"
    access_key = "datalake"
    secret_key = "datalake"
    bucket_name = "raw"

    interval = 30  # seconds

    while True:
        start_time = time.time()
        now = datetime.now()
        object_name = now.strftime("person_%Y-%m-%d-%H-%M-%S.json")
        extract_and_load(api_server, bucket_name, object_name, minio_endpoint, access_key, secret_key, secure=False)
        elapsed = time.time() - start_time
        sleep_time = max(0, interval - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)
    
if __name__ == "__main__":
    main()
