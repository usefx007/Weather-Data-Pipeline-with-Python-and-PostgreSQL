"""
Unit tests for safe atomic raw JSON storage, metadata wrapper format,
filename collision resistance, and error cleanup.
"""

import json
from pathlib import Path
from unittest.mock import patch
import pytest

import extract_weather


@pytest.fixture
def dummy_response():
    """Provides a dummy API response for storage testing."""
    return {
        "hourly": {
            "time": ["2026-10-21T00:00"],
            "temperature_2m": [21.0],
            "relative_humidity_2m": [50],
            "precipitation": [0.0],
            "wind_speed_10m": [11.0],
            "weather_code": [0],
        }
    }


def test_save_raw_weather_data_success(tmp_path, dummy_response):
    """Test saving raw data creates correct folder, file, and metadata wrapper."""
    params = {"latitude": 30.0444, "longitude": 31.2357}
    saved_file = extract_weather.save_raw_weather_data(
        city_key="cairo",
        original_api_response=dummy_response,
        request_params=params,
        start_date="2026-10-21",
        end_date="2026-11-19",
        base_dir=tmp_path,
    )

    assert saved_file.exists()
    assert saved_file.parent.name == "cairo"

    # Verify content
    with open(saved_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Check wrapper keys
    assert "metadata" in data
    assert "original_api_response" in data

    # Check metadata fields
    meta = data["metadata"]
    assert meta["city"] == "Cairo"
    assert meta["city_key"] == "cairo"
    assert meta["latitude"] == 30.0444
    assert meta["longitude"] == 31.2357
    assert meta["requested_start_date"] == "2026-10-21"
    assert meta["requested_end_date"] == "2026-11-19"
    assert "extracted_at_utc" in meta
    assert meta["source"] == "Open-Meteo Historical Weather API"
    assert meta["request_parameters"] == params

    # Check original payload untouched
    assert data["original_api_response"] == dummy_response


def test_save_raw_weather_data_collision_resistance(tmp_path, dummy_response):
    """Test that multiple writes for the same city generate distinct, non-colliding files."""
    file1 = extract_weather.save_raw_weather_data(
        city_key="alexandria",
        original_api_response=dummy_response,
        request_params={},
        start_date="2026-10-21",
        end_date="2026-11-19",
        base_dir=tmp_path,
    )

    file2 = extract_weather.save_raw_weather_data(
        city_key="alexandria",
        original_api_response=dummy_response,
        request_params={},
        start_date="2026-10-21",
        end_date="2026-11-19",
        base_dir=tmp_path,
    )

    assert file1 != file2
    assert file1.exists()
    assert file2.exists()


def test_save_raw_weather_data_cleanup_on_failure(tmp_path, dummy_response):
    """
    Test atomic writing failure with verified partial writing:
    Simulates writing a real initial chunk to the temporary file on disk,
    proves the partial file existed with bytes > 0, then raises OSError on next write.
    Verifies that:
      1. Exactly 2 write calls occurred: 1st wrote partial bytes, 2nd raised OSError.
      2. The temporary file (.tmp_*) is deleted after failure.
      3. No final file (weather_*.json) is created.
    """
    aswan_dir = tmp_path / "aswan"

    write_attempts = [0]
    bytes_written_before_failure = [0]
    tmp_file_existed_during_write = []

    real_open = open

    def faulty_open(file_path, mode="r", *args, **kwargs):
        fp = real_open(file_path, mode, *args, **kwargs)
        # Intercept write only on our temporary write files
        if "w" in mode and Path(file_path).name.startswith(".tmp_"):
            original_write = fp.write

            def faulty_write(chunk):
                write_attempts[0] += 1
                if write_attempts[0] == 1:
                    # Write initial chunk to disk and flush
                    res = original_write(chunk)
                    fp.flush()
                    bytes_written_before_failure[0] = Path(file_path).stat().st_size
                    tmp_file_existed_during_write.append(Path(file_path).exists())
                    return res
                # Mid-write failure on second write
                raise OSError("Simulated disk full / mid-stream write failure")

            fp.write = faulty_write
        return fp

    with patch("builtins.open", side_effect=faulty_open):
        with pytest.raises(OSError, match="Simulated disk full"):
            extract_weather.save_raw_weather_data(
                city_key="aswan",
                original_api_response=dummy_response,
                request_params={},
                start_date="2026-10-21",
                end_date="2026-11-19",
                base_dir=tmp_path,
            )

    # PROOF 1: Exactly 2 write calls took place; 1st succeeded with real bytes on disk, 2nd raised OSError
    assert write_attempts[0] == 2, f"Expected exactly 2 write calls, got {write_attempts[0]}"
    assert bytes_written_before_failure[0] > 0, "Expected partial bytes to be written to disk on 1st write"
    assert tmp_file_existed_during_write == [True], "Expected temporary file to exist on disk during 1st write"

    # PROOF 2 & 3: Direct assertions on directory glob - no final file, no temporary files left
    final_files = [f for f in aswan_dir.glob("*.json") if not f.name.startswith(".tmp_")]
    assert len(final_files) == 0, f"Final file should not be created on failure: {final_files}"

    tmp_files = list(aswan_dir.glob(".tmp_*"))
    assert len(tmp_files) == 0, f"Temporary files were not cleaned up: {tmp_files}"


def test_save_raw_weather_data_custom_city_metadata(tmp_path, dummy_response):
    """Test saving raw data when custom city metadata is supplied."""
    custom_city = {
        "display_name": "Luxor",
        "latitude": 25.6872,
        "longitude": 32.6396,
        "folder_name": "luxor",
    }

    saved_file = extract_weather.save_raw_weather_data(
        city_key="luxor",
        original_api_response=dummy_response,
        request_params={"latitude": 25.6872, "longitude": 32.6396},
        start_date="2026-10-21",
        end_date="2026-11-19",
        city_metadata=custom_city,
        base_dir=tmp_path,
    )

    assert saved_file.exists()
    assert saved_file.parent.name == "luxor"

    with open(saved_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["metadata"]["city"] == "Luxor"
    assert data["metadata"]["latitude"] == 25.6872
