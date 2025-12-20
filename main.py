

from services.write_data_to_minio import write_data_to_minio
from services.get_json_response import get_json_response
from services.extract_and_load import extract_and_load
import time
from datetime import datetime
        
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
        object_name = now.strftime("year=%Y/month=%m/day=%d/hour=%H/person_%M%S.json")
        extract_and_load(api_server, bucket_name, object_name, minio_endpoint, access_key, secret_key, secure=False)
        elapsed = time.time() - start_time
        sleep_time = max(0, interval - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)
    
if __name__ == "__main__":
    main()
