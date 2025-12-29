from extract_and_stage import extract_and_stage
import time
from datetime import datetime


def main():
    api_server = "https://randomuser.me/api/"
    bucket_name = "staging"
    main_folder = "people"

    interval = 30  # seconds

    while True:
        start_time = time.time()
        now = datetime.now()

        object_name = now.strftime(
            f"{main_folder}/year=%Y/month=%m/day=%d/hour=%H/person_%M%S.json"
        )
        extract_and_stage(
            api_server,
            bucket_name,
            object_name,
        )
        elapsed = time.time() - start_time
        sleep_time = max(0, interval - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)


if __name__ == "__main__":
    main()
