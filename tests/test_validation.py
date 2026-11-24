"""
Unit tests for API response payload validation, structural integrity checks,
array length consistency, and handling of legitimate missing values.
"""

import pytest
import extract_weather


@pytest.fixture
def valid_payload():
    """Returns a valid Open-Meteo response structure."""
    return {
        "hourly": {
            "time": ["2026-10-21T00:00", "2026-10-21T01:00", "2026-10-21T02:00"],
            "temperature_2m": [22.0, 21.5, 20.8],
            "relative_humidity_2m": [55, 60, 62],
            "precipitation": [0.0, 0.0, 0.1],
            "wind_speed_10m": [14.2, 12.0, 9.8],
            "weather_code": [0, 1, 2],
        }
    }


def test_validate_weather_payload_valid(valid_payload):
    """Test that a well-structured payload passes validation."""
    assert extract_weather.validate_weather_payload(valid_payload) is True


def test_validate_weather_payload_not_dict():
    """Test that non-dict payloads are rejected."""
    with pytest.raises(ValueError, match="Expected JSON object, got list"):
        extract_weather.validate_weather_payload(["not", "a", "dict"])

    with pytest.raises(ValueError, match="Expected JSON object, got str"):
        extract_weather.validate_weather_payload("raw string")


def test_validate_weather_payload_missing_hourly():
    """Test that payloads missing the 'hourly' section are rejected."""
    payload = {"latitude": 30.0, "longitude": 31.0}
    with pytest.raises(ValueError, match="missing required 'hourly' object"):
        extract_weather.validate_weather_payload(payload)


def test_validate_weather_payload_missing_time():
    """Test that payloads missing the 'time' array are rejected."""
    payload = {
        "hourly": {
            "temperature_2m": [20.0],
            "relative_humidity_2m": [50],
            "precipitation": [0.0],
            "wind_speed_10m": [10.0],
            "weather_code": [0],
        }
    }
    with pytest.raises(ValueError, match="missing 'time' array"):
        extract_weather.validate_weather_payload(payload)


def test_validate_weather_payload_missing_weather_variable(valid_payload):
    """Test rejection when a required weather variable is missing."""
    del valid_payload["hourly"]["precipitation"]
    with pytest.raises(ValueError, match="missing expected variable 'precipitation'"):
        extract_weather.validate_weather_payload(valid_payload)


def test_validate_weather_payload_empty_time(valid_payload):
    """Test rejection when 'time' array has no observations."""
    for key in valid_payload["hourly"]:
        valid_payload["hourly"][key] = []

    with pytest.raises(ValueError, match="Hourly 'time' array is empty"):
        extract_weather.validate_weather_payload(valid_payload)


def test_validate_weather_payload_length_mismatch(valid_payload):
    """Test rejection when hourly variable array length doesn't match 'time' length."""
    # time has 3 elements, make temperature have only 2
    valid_payload["hourly"]["temperature_2m"] = [22.0, 21.5]

    with pytest.raises(ValueError, match="Array length mismatch"):
        extract_weather.validate_weather_payload(valid_payload)


def test_validate_weather_payload_preserves_legitimate_null_values(valid_payload):
    """
    Test that legitimate missing observation values (None/null) inside arrays
    do NOT cause validation to fail. Member 2 owns cleaning missing values.
    """
    # Simulate missing measurements in precipitation and relative humidity
    valid_payload["hourly"]["precipitation"] = [None, 0.0, None]
    valid_payload["hourly"]["temperature_2m"] = [22.0, None, 20.8]

    # Must pass validation without error
    assert extract_weather.validate_weather_payload(valid_payload) is True
