import pytest
from airflow.operators.python import PythonOperator
import pendulum
from dags import newyorktaxi_dag
import responses
import requests
from unittest.mock import patch, Mock

def test_generate_metadata():
    test = PythonOperator(
        task_id="test", python_callable=newyorktaxi_dag.generate_metadata
    )
    result = test.execute(
        context={
            "data_interval_start": pendulum.datetime(
                2023, 1, 1, tz="Australia/Sydney"
            ).in_timezone("UTC")
        }
    )
    assert result == [
        newyorktaxi_dag.Metadata("yellow", "2023", "01"),
        newyorktaxi_dag.Metadata("green", "2023", "01"),
        newyorktaxi_dag.Metadata("fhv", "2023", "01"),
        newyorktaxi_dag.Metadata("fhvhv", "2023", "01"),
    ]

    urls = [metadata.url for metadata in result]
    assert urls == [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2023-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2023-01.parquet",
    ]


def test_generate_metadata_end_of_month():
    test = PythonOperator(
        task_id="test", python_callable=newyorktaxi_dag.generate_metadata
    )
    result = test.execute(
        context={
            "data_interval_start": pendulum.datetime(
                2023, 2, 28, tz="Australia/Sydney"
            ).in_timezone("UTC")
        }
    )
    assert result == [
        newyorktaxi_dag.Metadata("yellow", "2023", "02"),
        newyorktaxi_dag.Metadata("green", "2023", "02"),
        newyorktaxi_dag.Metadata("fhv", "2023", "02"),
        newyorktaxi_dag.Metadata("fhvhv", "2023", "02"),
    ]

    urls = [metadata.url for metadata in result]
    assert urls == [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2023-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2023-02.parquet",
    ]


@pytest.fixture
def url():
    return newyorktaxi_dag.Metadata("yellow", "2023", "01").url


@pytest.fixture
def test():
    return PythonOperator(task_id="test", python_callable=newyorktaxi_dag.api_call)


@responses.activate
def test_api_call_success(url, test):
    rsp1 = responses.get(
        url=url,
        body=b'PAR1\x15\x04\x15\x10\x15"L\x15\x04\x15\0\x12\0\0PAR1',
        status=200,
    )

    response = test.execute(context={"url": url, "backoff_factor": 0.1})
    assert isinstance(response, requests.models.Response)
    assert response.status_code == 200
    assert type(response.content) == bytes
    assert rsp1.call_count == 1


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_api_call_max_retries_success(url, test):
    rsp1 = responses.get(url, status=503)
    rsp2 = responses.get(url, status=503)
    rsp3 = responses.get(url, status=200)

    response = test.execute(
        context={"url": url, "backoff_factor": 0.1, "max_HTTP_retries": 2}
    )
    assert response.status_code == 200
    assert rsp1.call_count == 1
    assert rsp2.call_count == 1
    assert rsp3.call_count == 1


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_api_call_max_retries_exceeded(url, test):
    rsp1 = responses.get(url, status=503)
    rsp2 = responses.get(url, status=503)
    rsp3 = responses.get(url, status=503)

    with pytest.raises(requests.exceptions.RetryError):
        response = test.execute(
            context={"url": url, "backoff_factor": 0.1, "max_HTTP_retries": 2}
        )

    assert rsp1.call_count == 1
    assert rsp2.call_count == 1
    assert rsp3.call_count == 1


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_api_call_other_status_max_retries_success(url, test):
    rsp1 = responses.get(url, status=404)
    rsp2 = responses.get(url, status=404)
    rsp3 = responses.get(url, status=200)

    response = test.execute(
        context={"url": url, "backoff_factor": 0.1, "max_HTTP_retries": 2}
    )

    assert response.status_code == 200
    assert rsp1.call_count == 1
    assert rsp2.call_count == 1
    assert rsp3.call_count == 1


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_api_call_other_status_max_retries_exceeded(url, test):
    rsp1 = responses.get(url, status=404)
    rsp2 = responses.get(url, status=404)
    rsp3 = responses.get(url, status=404)

    with pytest.raises(requests.exceptions.HTTPError):
        response = test.execute(
            context={"url": url, "backoff_factor": 0.1, "max_HTTP_retries": 2}
        )

    assert rsp1.call_count == 1
    assert rsp2.call_count == 1
    assert rsp3.call_count == 1


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_api_call_retry_logic(url, test):
    rsp1 = responses.get(url, status=404)
    rsp2 = responses.get(url, status=404)
    rsp3 = responses.get(url, status=503)
    rsp4 = responses.get(url, status=503)
    rsp5 = responses.get(url, status=503)
    rsp6 = responses.get(url, status=503)

    with pytest.raises(requests.exceptions.RetryError):
        response = test.execute(
            context={
                "url": url,
                "backoff_factor": 0.1,
                "max_HTTP_retries": 3,
                "max_other_retry": 2,
            }
        )

    assert rsp1.call_count == 1
    assert rsp2.call_count == 1
    assert rsp3.call_count == 1
    assert rsp4.call_count == 1
    assert rsp5.call_count == 1
    assert rsp6.call_count == 1


@responses.activate()
def test_api_call_connection_error(url, test):
    rsp1 = responses.get(
        url=url,
        body=requests.exceptions.ConnectionError(),
    )

    max_other_retry = 2

    with pytest.raises(requests.exceptions.ConnectionError):
        response = test.execute(
            context={
                "url": url,
                "backoff_factor": 0.1,
                "max_HTTP_retries": 3,
                "max_other_retry": max_other_retry,
            }
        )

    assert rsp1.call_count == max_other_retry + 1


@responses.activate()
def test_api_call_timeout_error(url, test):
    rsp1 = responses.get(
        url=url,
        body=requests.exceptions.Timeout(),
    )

    max_other_retry = 2

    with pytest.raises(requests.exceptions.Timeout):
        response = test.execute(
            context={
                "url": url,
                "backoff_factor": 0.1,
                "max_HTTP_retries": 3,
                "max_other_retry": max_other_retry,
            }
        )

    assert rsp1.call_count == max_other_retry + 1

@patch('dags.newyorktaxi_dag.Path')
def test_create_folder(mock_path):
    newyorktaxi_dag.create_folder()

    mock_path.assert_called_once_with('data')
    mock_path.return_value.mkdir.assert_called_once_with(exist_ok=True)

@patch('dags.newyorktaxi_dag.open', create=True)
def test_write_data(mock_open):
    mock_response = Mock()
    mock_response.content = b"test"
    newyorktaxi_dag.write_data(mock_response, newyorktaxi_dag.Metadata("yellow", "2023", "02"))

    mock_open.assert_called_once_with('data/yellow-2023-02.parquet','wb')
    assert mock_response.content == b"test"