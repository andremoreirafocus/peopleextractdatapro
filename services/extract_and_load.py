from services.get_json_response import get_json_response
from services.write_data_to_minio import write_data_to_minio

def extract_and_load(api_server, bucket_name, object_name, minio_endpoint, access_key, secret_key, secure=True):
    print("extracting data from API server")
    data = get_json_response(api_server)
    if data:
        print(f"Data extracted {data}")
        write_data_to_minio(data, bucket_name, object_name, minio_endpoint, access_key, secret_key, secure)
