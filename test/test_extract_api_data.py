import pytest
import responses
from dags import extract_api_data
import requests


@pytest.fixture
def url():
    return "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet"


@responses.activate
def test_api_call_success(url):
    rsp1 = responses.get(
        url=url,
        body=b'PAR1\x15\x04\x15\x10\x15"L\x15\x04\x15\0\x12\0\0PAR1',
        status=200,
    )
    response = extract_api_data.api_call(url, backoff_factor=0.1)
    assert isinstance(response, requests.models.Response)
    assert response.status_code == 200
    assert type(response.content) == bytes
    assert rsp1.call_count == 1


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_api_call_max_retries_success(url):
    rsp1 = responses.get(url, status=503)
    rsp2 = responses.get(url, status=503)
    rsp3 = responses.get(url, status=200)

    response = extract_api_data.api_call(url, backoff_factor=0.1, max_HTTP_retries=2)

    assert response.status_code == 200
    assert rsp1.call_count == 1
    assert rsp2.call_count == 1
    assert rsp3.call_count == 1


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_api_call_max_retries_exceeded(url):
    rsp1 = responses.get(url, status=503)
    rsp2 = responses.get(url, status=503)
    rsp3 = responses.get(url, status=503)

    with pytest.raises(requests.exceptions.RetryError):
        response = extract_api_data.api_call(
            url, backoff_factor=0.1, max_HTTP_retries=2
        )

    assert rsp1.call_count == 1
    assert rsp2.call_count == 1
    assert rsp3.call_count == 1


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_api_call_other_status_max_retries_success(url):
    rsp1 = responses.get(url, status=404)
    rsp2 = responses.get(url, status=404)
    rsp3 = responses.get(url, status=200)

    response = extract_api_data.api_call(url, backoff_factor=0.1, max_HTTP_retries=2)

    assert response.status_code == 200
    assert rsp1.call_count == 1
    assert rsp2.call_count == 1
    assert rsp3.call_count == 1


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_api_call_other_status_max_retries_exceeded(url):
    rsp1 = responses.get(url, status=404)
    rsp2 = responses.get(url, status=404)
    rsp3 = responses.get(url, status=404)

    with pytest.raises(requests.exceptions.HTTPError):
        response = extract_api_data.api_call(
            url, backoff_factor=0.1, max_HTTP_retries=2
        )

    assert rsp1.call_count == 1
    assert rsp2.call_count == 1
    assert rsp3.call_count == 1


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_api_call_retry_logic(url):
    rsp1 = responses.get(url, status=404)
    rsp2 = responses.get(url, status=404)
    rsp3 = responses.get(url, status=503)
    rsp4 = responses.get(url, status=503)
    rsp5 = responses.get(url, status=503)
    rsp6 = responses.get(url, status=503)

    with pytest.raises(requests.exceptions.RetryError):
        response = extract_api_data.api_call(
            url, backoff_factor=0.1, max_HTTP_retries=3, max_other_retry=2
        )

    assert rsp1.call_count == 1
    assert rsp2.call_count == 1
    assert rsp3.call_count == 1
    assert rsp4.call_count == 1
    assert rsp5.call_count == 1
    assert rsp6.call_count == 1


@responses.activate()
def test_api_call_connection_error(url):
    rsp1 = responses.get(
        url=url,
        body=requests.exceptions.ConnectionError(),
    )

    max_other_retry = 2

    with pytest.raises(requests.exceptions.ConnectionError):
        response = extract_api_data.api_call(
            url, backoff_factor=0.1, max_HTTP_retries=3, max_other_retry=max_other_retry
        )

    assert rsp1.call_count == max_other_retry + 1


@responses.activate()
def test_api_call_timeout_error(url):
    rsp1 = responses.get(
        url=url,
        body=requests.exceptions.Timeout(),
    )

    max_other_retry = 2

    with pytest.raises(requests.exceptions.Timeout):
        response = extract_api_data.api_call(
            url, backoff_factor=0.1, max_HTTP_retries=3, max_other_retry=max_other_retry
        )

    assert rsp1.call_count == max_other_retry + 1
