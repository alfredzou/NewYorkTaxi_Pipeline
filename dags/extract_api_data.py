from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import requests
from requests.adapters import HTTPAdapter, Retry
import logging
import itertools
import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="log.log",
    filemode="a",
)

logger = logging.getLogger()

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2023, 1, 1, tz="Australia/Sydney"),
    "end_date": pendulum.datetime(2023, 12, 31, tz="Australia/Sydney"),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}


def generate_api_urls(**kwargs) -> list[tuple[str, str, str, str]]:
    # Convert UTC to Australia/Sydney timezone
    data_interval_start: pendulum.DateTime = kwargs["data_interval_start"]
    data_interval_start = data_interval_start.in_timezone("Australia/Sydney")

    types: list[str] = ["yellow", "green", "fhv", "fhvhv"]
    years: list[str] = [str(data_interval_start.year)]
    months: list[str] = [f"{data_interval_start.month:02}"]
    return [
        (
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{year}-{month}.parquet",
            type,
            year,
            month,
        )
        for type, year, month in itertools.product(types, years, months)
    ]


def api_call(
    url: str,
    max_HTTP_retries: int = 5,
    max_other_retry: int = 2,
    backoff_factor: int = 1,
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


def download_api_data(
    response: requests.Response, type: str, year: str, month: str
) -> None:
    with open(f"{type}-{year}-{month}.parquet", "wb") as f:
        f.write(response.content)
    logger.info("file downloaded successfully")


def api_calls(**kwargs):
    api_urls: list[tuple[str, str, str, str]] = generate_api_urls(**kwargs)
    for url, type, year, month in api_urls:
        logger.info(f"{type}-{year}-{month}:Attempting connection")
        response = api_call(url)
        logger.info(f"{type}-{year}-{month}:Successfully connected")
        download_api_data(response, type, year, month)
        logger.info(f"{type}-{year}-{month}:Successfully downloaded")


# Instantiate your DAG
with DAG(
    "newyorktaxi_dag",
    default_args=default_args,
    schedule="@monthly",
) as dag:
    task_0 = PythonOperator(
        task_id="api_calls",
        python_callable=api_calls,
    )

task_0
