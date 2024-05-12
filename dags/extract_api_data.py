# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
import pendulum
from datetime import timedelta
import requests
from requests.adapters import HTTPAdapter, Retry
import logging
import itertools
import time
# from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="log.log",
    filemode="a",
)

logger = logging.getLogger()

# default_args = {
#     'owner': 'airflow',
#     'start_date': pendulum.datetime(2023, 1, 1, tz="Australia/Sydney"),
#     'retries': 5,
#     'retry_delay': timedelta(minutes=1),
#     'catchup': True
# }


def download_api_data(
    response: requests.Response, type: str, year: str, month: str
) -> None:
    with open(f"{type}-{year}-{month}.parquet", "wb") as f:
        f.write(response.content)
    logger.info("file downloaded successfully")


def api_call(
    url: str, max_HTTP_retries: int = 5, max_other_retry:int = 2, backoff_factor: int = 1
) -> requests.Response:
    session = requests.Session()
    status_codes = [429, 502, 503, 504]
    retries = Retry(
        total=max_HTTP_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_codes,
        raise_on_status=True,
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    backoffs = [backoff_factor * (2**n) for n in range(max_other_retry + 1)]
    for run, backoff in enumerate(backoffs, 1):
        try:
            response = session.get(url, timeout=5)
            response.raise_for_status()
            return response       
        except requests.exceptions.RetryError as e:
            logging.error(f"Max retries reached for {status_codes}: {e}")
            raise
        except requests.exceptions.HTTPError as e:
            logging.error(f"Run {run}. HTTP Error: {e}")
            if run == max_other_retry + 1:
                logging.error(f"Max retries reached: {e}")
                raise
        except requests.exceptions.Timeout as e:
            logging.error(f"Run {run}. Timeout Error: {e}")
            if run == max_other_retry + 1:
                logging.error(f"Max retries reached: {e}")
                raise
        except Exception as e:
            logging.error(f"Run {run}. Error: {e}")
            logging.error(type(e))
            if run == max_other_retry + 1:
                logging.error(f"Max retries reached: {e}")
                raise

        logging.info(f"Attempting run {run+1} with {backoff} seconds delay")
        time.sleep(backoff)


if __name__ == "__main__":
    years = ["2023"]
    months = [f"{i:02}" for i in range(1, 13)]
    types = ["yellow", "green", "fhv", "fhvhv"]
    for year, month, type in itertools.product(years, months, types):
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{year}-{month}.parquet"
        response = api_call(url)
        download_api_data(response, year, month, type)


# def task1():
#     print('helloworld')
#     # import requests
#     year = 2023
#     month = '01'
#     r = requests.get(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet')
#     print(r.status_code)

# def task2():
#     return "Executing Task 2"

# # Instantiate your DAG
# with DAG(
#     'my_first_dagsv2',
#     default_args=default_args,
#     schedule_interval='@monthly',
# ) as dag:
#     task_1 = PythonOperator(
#         task_id='task_1',
#         python_callable=task1,
#     )
#     task_2 = PythonOperator(
#         task_id='task_2',
#         python_callable=task2,
#     )

# # Set task dependencies
# task_1 >> task_2
