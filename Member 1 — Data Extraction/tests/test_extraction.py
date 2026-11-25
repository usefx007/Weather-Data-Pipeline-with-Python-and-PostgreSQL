"""
Unit tests for the weather data extraction logic, error handling,
retry mechanisms, and failure isolation using pytest monkeypatch.
"""

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock
import pytest
import requests

import config
import extract_weather


@pytest.fixture
def sample_api_response():
    """Provides a valid minimal API response payload."""
    return {
        "latitude": 30.0,
        "longitude": 31.25,
        "generationtime_ms": 0.5,
        "utc_offset_seconds": 0,
        "timezone": "UTC",
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "°C",
            "relative_humidity_2m": "%",
            "precipitation": "mm",
            "wind_speed_10m": "km/h",
            "weather_code": "wmo code",
        },
        "hourly": {
            "time": ["2026-10-21T00:00", "2026-10-21T01:00"],
            "temperature_2m": [20.5, 19.8],
            "relative_humidity_2m": [60, 65],
            "precipitation": [0.0, 0.0],
            "wind_speed_10m": [12.0, 10.5],
            "weather_code": [0, 1],
        },
    }


def test_parse_retry_after_direct(monkeypatch):
    """
    Directly tests parse_retry_after() without mocking the function itself.
    Uses frozen/anchored datetime in extract_weather to guarantee 100% deterministic results.
    """
    # 1. Numeric seconds string
    assert extract_weather.parse_retry_after("120", default_wait=1.0) == 120.0
    assert extract_weather.parse_retry_after(" 45 ", default_wait=1.0) == 45.0
    assert extract_weather.parse_retry_after("0", default_wait=1.0) == 0.0

    # 2. None / Empty string falls back to default
    assert extract_weather.parse_retry_after(None, default_wait=3.5) == 3.5
    assert extract_weather.parse_retry_after("", default_wait=3.5) == 3.5

    # 3. Deterministic Future and Past HTTP-date tests
    fixed_now = datetime(2026, 10, 21, 12, 0, 0, tzinfo=timezone.utc)

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(extract_weather, "datetime", FrozenDatetime)

    # Future date: exactly 10 seconds ahead -> wait must be exactly 10.0s
    future_date_str = "Wed, 21 Oct 2026 12:00:10 GMT"
    wait_future = extract_weather.parse_retry_after(future_date_str, default_wait=1.0)
    assert wait_future == 10.0

    # Past date: 30 seconds in the past -> must clamp to 0.0s
    past_date_str = "Wed, 21 Oct 2026 11:59:30 GMT"
    wait_past = extract_weather.parse_retry_after(past_date_str, default_wait=1.0)
    assert wait_past == 0.0

    # 4. Malformed date string falls back to default_wait
    assert extract_weather.parse_retry_after("invalid-http-date-value", default_wait=7.0) == 7.0


def test_fetch_weather_data_success(monkeypatch, sample_api_response):
    """Test successful extraction on the first attempt."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_api_response

    mock_session = MagicMock()
    mock_session.get.return_value = mock_response

    data = extract_weather.fetch_weather_data(
        session=mock_session,
        params={"latitude": 30.0, "longitude": 31.0},
        city_name="TestCity",
        max_retries=3,
    )

    assert data == sample_api_response
    assert mock_session.get.call_count == 1


def test_fetch_weather_data_retry_429_numeric(monkeypatch, sample_api_response):
    """Test handling HTTP 429 with numeric Retry-After header and backoff."""
    mock_429 = MagicMock()
    mock_429.status_code = 429
    mock_429.headers = {"Retry-After": "2"}

    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.json.return_value = sample_api_response

    mock_session = MagicMock()
    mock_session.get.side_effect = [mock_429, mock_200]

    sleep_calls = []
    monkeypatch.setattr(extract_weather.time, "sleep", lambda s: sleep_calls.append(s))

    data = extract_weather.fetch_weather_data(
        session=mock_session,
        params={},
        city_name="TestCity",
        max_retries=3,
        initial_backoff=1.0,
    )

    assert data == sample_api_response
    assert mock_session.get.call_count == 2
    assert sleep_calls == [2.0]


def test_fetch_weather_data_retry_429_http_date(monkeypatch, sample_api_response):
    """
    Test handling HTTP 429 with actual HTTP-date Retry-After header
    without mocking parse_retry_after itself, using frozen time for deterministic assertion.
    """
    fixed_now = datetime(2026, 10, 21, 12, 0, 0, tzinfo=timezone.utc)

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(extract_weather, "datetime", FrozenDatetime)

    # Exactly 8 seconds in the future
    http_date_str = "Wed, 21 Oct 2026 12:00:08 GMT"

    mock_429 = MagicMock()
    mock_429.status_code = 429
    mock_429.headers = {"Retry-After": http_date_str}

    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.json.return_value = sample_api_response

    mock_session = MagicMock()
    mock_session.get.side_effect = [mock_429, mock_200]

    sleep_calls = []
    monkeypatch.setattr(extract_weather.time, "sleep", lambda s: sleep_calls.append(s))

    data = extract_weather.fetch_weather_data(
        session=mock_session,
        params={},
        city_name="TestCity",
        max_retries=3,
    )

    assert data == sample_api_response
    assert mock_session.get.call_count == 2
    assert sleep_calls == [8.0]


@pytest.mark.parametrize("status_code", [500, 502, 503, 504])
def test_fetch_weather_data_server_errors_parametrized(
    monkeypatch,
    sample_api_response,
    status_code,
):
    """
    Independently tests each transient server error code (500, 502, 503, 504)
    using default production settings (initial_backoff=1.0, backoff_factor=2.0, max_retries=3).
    Verifies that response sequence [error, error, success] results in 3 total requests
    and exact backoff sleeps of [1.0, 2.0].
    """
    mock_err = MagicMock()
    mock_err.status_code = status_code

    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.json.return_value = sample_api_response

    mock_session = MagicMock()
    mock_session.get.side_effect = [mock_err, mock_err, mock_200]

    sleep_calls = []
    monkeypatch.setattr(extract_weather.time, "sleep", lambda s: sleep_calls.append(s))

    data = extract_weather.fetch_weather_data(
        session=mock_session,
        params={},
        city_name="TestCity",
        max_retries=3,
        initial_backoff=1.0,
        backoff_factor=2.0,
    )

    assert data == sample_api_response
    # Exactly 3 attempts: 1st error, 2nd error, 3rd success
    assert mock_session.get.call_count == 3
    assert sleep_calls == [1.0, 2.0]


def test_fetch_weather_data_network_error_retry(monkeypatch, sample_api_response):
    """Test retry on connection errors and timeouts with exact backoff verification."""
    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.json.return_value = sample_api_response

    mock_session = MagicMock()
    mock_session.get.side_effect = [
        requests.exceptions.Timeout("Connection timed out"),
        requests.exceptions.ConnectionError("DNS failure"),
        mock_200,
    ]

    sleep_calls = []
    monkeypatch.setattr(extract_weather.time, "sleep", lambda s: sleep_calls.append(s))

    data = extract_weather.fetch_weather_data(
        session=mock_session,
        params={},
        city_name="TestCity",
        max_retries=3,
        initial_backoff=2.0,
        backoff_factor=3.0,
    )

    assert data == sample_api_response
    assert mock_session.get.call_count == 3
    # Exact exponential backoff values: 2.0 * (3.0^0) = 2.0, 2.0 * (3.0^1) = 6.0
    assert sleep_calls == [2.0, 6.0]


def test_fetch_weather_data_permanent_4xx_fail_fast():
    """Test that permanent 400 Bad Request fails fast without retrying."""
    mock_400 = MagicMock()
    mock_400.status_code = 400
    mock_400.text = "Invalid latitude value"
    mock_400.raise_for_status.side_effect = requests.exceptions.HTTPError("400 Client Error")

    mock_session = MagicMock()
    mock_session.get.return_value = mock_400

    with pytest.raises(requests.exceptions.HTTPError):
        extract_weather.fetch_weather_data(
            session=mock_session,
            params={"latitude": 999.0},
            city_name="InvalidCity",
            max_retries=3,
        )

    # Must fail on attempt 1 without retries
    assert mock_session.get.call_count == 1


def test_fetch_weather_data_permanent_404_fail_fast():
    """Test that permanent 404 Not Found fails fast on attempt 1 without retrying."""
    mock_404 = MagicMock()
    mock_404.status_code = 404
    mock_404.text = "Resource Not Found"
    mock_404.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")

    mock_session = MagicMock()
    mock_session.get.return_value = mock_404

    with pytest.raises(requests.exceptions.HTTPError):
        extract_weather.fetch_weather_data(
            session=mock_session,
            params={"latitude": 30.0},
            city_name="NotFoundCity",
            max_retries=3,
        )

    assert mock_session.get.call_count == 1


def test_fetch_weather_data_malformed_json():
    """Test that invalid/malformed JSON in response body raises ValueError."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Expecting value: line 1 column 1 (char 0)")

    mock_session = MagicMock()
    mock_session.get.return_value = mock_response

    with pytest.raises(ValueError, match="Malformed JSON from API"):
        extract_weather.fetch_weather_data(
            session=mock_session,
            params={},
            city_name="CorruptCity",
            max_retries=3,
        )


def test_fetch_weather_data_invalid_retry_parameters():
    """Test validation of retry parameters."""
    mock_session = MagicMock()
    with pytest.raises(ValueError, match="max_retries must be at least 1"):
        extract_weather.fetch_weather_data(mock_session, {}, "City", max_retries=0)

    with pytest.raises(ValueError, match="initial_backoff must be non-negative"):
        extract_weather.fetch_weather_data(mock_session, {}, "City", initial_backoff=-1.0)

    with pytest.raises(ValueError, match="backoff_factor must be at least 1"):
        extract_weather.fetch_weather_data(mock_session, {}, "City", backoff_factor=0.5)


def test_failure_isolation_in_extract_all_cities(monkeypatch, tmp_path):
    """Test that failure in one city does not stop or abort remaining cities."""
    test_cities = {
        "city_fail": {
            "display_name": "FailingCity",
            "latitude": 10.0,
            "longitude": 10.0,
            "folder_name": "failing_city",
        },
        "city_success": {
            "display_name": "SuccessfulCity",
            "latitude": 20.0,
            "longitude": 20.0,
            "folder_name": "successful_city",
        },
    }

    def fake_extract_city(city_key, session, start_date, end_date, city_metadata=None):
        if city_key == "city_fail":
            return False, None, "Simulated 500 server outage"
        saved = tmp_path / "successful_city" / "sample.json"
        saved.parent.mkdir(parents=True, exist_ok=True)
        saved.write_text("{}", encoding="utf-8")
        return True, saved, None

    monkeypatch.setattr(extract_weather, "extract_city", fake_extract_city)

    summary = extract_weather.extract_all_cities(
        start_date="2026-10-01",
        end_date="2026-10-30",
        target_cities=test_cities,
    )

    assert summary["is_fully_successful"] is False
    assert summary["successful_cities"] == ["SuccessfulCity"]
    assert summary["failed_cities"] == ["FailingCity"]
    assert "FailingCity" in summary["errors"]
    assert len(summary["saved_files"]) == 1


def test_extract_all_cities_empty_target_cities():
    """Test behavior when target_cities is empty."""
    summary = extract_weather.extract_all_cities(
        start_date="2026-10-01",
        end_date="2026-10-30",
        target_cities={},
    )
    assert summary["total_cities"] == 0
    assert summary["successful_cities"] == []
    assert summary["failed_cities"] == []
    assert summary["is_fully_successful"] is True


def test_date_range_validation():
    """Test validation of start_date and end_date inputs."""
    # Only start_date provided
    with pytest.raises(ValueError, match="Incomplete date range provided"):
        extract_weather.extract_all_cities(start_date="2026-10-01", end_date=None)

    # Inverted date range
    with pytest.raises(ValueError, match="cannot be after end_date"):
        extract_weather.extract_all_cities(start_date="2026-11-01", end_date="2026-10-01")

    # Malformed date format
    with pytest.raises(ValueError, match="Invalid date format"):
        extract_weather.extract_all_cities(start_date="2026/10/01", end_date="2026-10-30")
