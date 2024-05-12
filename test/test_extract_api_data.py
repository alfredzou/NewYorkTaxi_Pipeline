import pytest
from unittest import mock
from dags import extract_api_data
import requests

@pytest.fixture
def url():
    return "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet"

@mock.patch("requests.Session.get")
def test_api_call_success(mock_session_get, url):
    mock_session_response = requests.models.Response()
    mock_session_response.status_code = 200
    mock_session_get.return_value = mock_session_response

    response = extract_api_data.api_call(url, backoff_factor=0.1)
    assert isinstance(response,requests.models.Response)

@mock.patch("requests.Session.get")
def test_api_call_HTTPError(mock_session_get, url):
    mock_session_response = requests.models.Response()
    mock_session_response.status_code = 404
    mock_session_get.return_value = mock_session_response

    with pytest.raises(requests.exceptions.HTTPError):
        extract_api_data.api_call(url, backoff_factor=0.1)

@mock.patch("requests.Session.get")
def test_api_call_RetryError(mock_session_get, url):
    mock_session_response = mock.Mock(status_code = 429)
    mock_session_response.raise_for_status.side_effect = requests.exceptions.RetryError
    mock_session_get.return_value = mock_session_response

    with pytest.raises(requests.exceptions.RetryError):
        extract_api_data.api_call(url, backoff_factor=0.1)

@mock.patch("requests.Session.get", side_effect=requests.exceptions.Timeout)
def test_api_call_timeout(mock_session_get, url):
    with pytest.raises(requests.exceptions.Timeout):
        extract_api_data.api_call(url, backoff_factor=0.1)