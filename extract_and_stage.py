from services.fetch_data import fetch_data
from services.write_data_to_minio import write_data_to_minio
from services.get_minio_connection_data import get_minio_connection_data
import logging


def extract_and_stage(api_server, bucket_name, object_name):
    print("extracting data from API server")
    logging.info("Starting data extraction from API server")
    data = fetch_data(api_server)
    if data:
        print(f"Data extracted {data}")
        logging.info("Data extraction successful, proceeding to load data into MinIO")
        connection_data = get_minio_connection_data()
        write_data_to_minio(connection_data, bucket_name, object_name, data)
        logging.info("Data loading completed successfully")
    else:
        print("No data extracted from API server")
        logging.warning("Data extraction failed, no data to load into MinIO")
