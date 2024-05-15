from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import requests
from requests.adapters import HTTPAdapter, Retry
import logging
import itertools
import time
from dataclasses import dataclass
from pathlib import Path

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


@dataclass()
class Metadata:
    type: str
    year: str
    month: str

    def __post_init__(self) -> None:
        self.url: str = (
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/{self.type}_tripdata_{self.year}-{self.month}.parquet"
        )


def generate_metadata(**kwargs) -> list[Metadata]:
    # Convert UTC to Australia/Sydney timezone
    data_interval_start: pendulum.DateTime = kwargs["data_interval_start"]
    data_interval_start = data_interval_start.in_timezone("Australia/Sydney")

    types: list[str] = ["yellow", "green", "fhv", "fhvhv"]
    years: list[str] = [str(data_interval_start.year)]
    months: list[str] = [f"{data_interval_start.month:02}"]
    return [
        Metadata(type, year, month)
        for type, year, month in itertools.product(types, years, months)
    ]

def create_folder() -> None:
    Path("data").mkdir(exist_ok=True)

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


def write_data(response: requests.Response, metadata: Metadata) -> None:
    path:str = f"data/{metadata.type}-{metadata.year}-{metadata.month}.parquet"
    with open(path, "wb") as f:
        f.write(response.content)
    logger.info(f"{metadata.type}-{metadata.year}-{metadata.month}:file downloaded successfully to {path}")


def api_calls(**kwargs) -> None:
    metadatas: Metadata = generate_metadata(**kwargs)
    create_folder()
    for metadata in metadatas:
        logger.info(
            f"{metadata.type}-{metadata.year}-{metadata.month}:Attempting connection"
        )
        response = api_call(metadata.url)
        logger.info(
            f"{metadata.type}-{metadata.year}-{metadata.month}:Successfully connected"
        )
        write_data(response, metadata)
        logger.info(
            f"{metadata.type}-{metadata.year}-{metadata.month}:Successfully downloaded"
        )


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
