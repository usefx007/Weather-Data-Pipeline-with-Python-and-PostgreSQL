"""
Configuration module for the Weather Data Extraction Pipeline.
Centralizes API endpoints, geographic coordinates, weather variables,
date range calculations, and file paths.
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple


# --- API Configuration ---
API_BASE_URL: str = "https://archive-api.open-meteo.com/v1/archive"
REQUEST_TIMEOUT_SECONDS: int = 15
MAX_RETRIES: int = 3
INITIAL_BACKOFF_SECONDS: float = 1.0
BACKOFF_FACTOR: float = 2.0

# --- Historical Date Calculation Settings ---
HISTORICAL_DAYS: int = 30
AVAILABILITY_BUFFER_DAYS: int = 5

# --- Weather Variables and Units ---
# Open-Meteo accepts variables as comma-separated values in the 'hourly' parameter
HOURLY_VARIABLES: list[str] = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]

TEMPERATURE_UNIT: str = "celsius"
WIND_SPEED_UNIT: str = "kmh"
PRECIPITATION_UNIT: str = "mm"

# --- Target Egyptian Cities Coordinates ---
CITIES: Dict[str, Dict[str, Any]] = {
    "cairo": {
        "display_name": "Cairo",
        "latitude": 30.0444,
        "longitude": 31.2357,
        "folder_name": "cairo",
    },
    "alexandria": {
        "display_name": "Alexandria",
        "latitude": 31.2001,
        "longitude": 29.9187,
        "folder_name": "alexandria",
    },
    "mansoura": {
        "display_name": "Mansoura",
        "latitude": 31.0409,
        "longitude": 31.3785,
        "folder_name": "mansoura",
    },
    "aswan": {
        "display_name": "Aswan",
        "latitude": 24.0889,
        "longitude": 32.8998,
        "folder_name": "aswan",
    },
    "port_said": {
        "display_name": "Port Said",
        "latitude": 31.2653,
        "longitude": 32.3019,
        "folder_name": "port_said",
    },
}

# --- Directory Paths ---
PROJECT_ROOT: Path = Path(__file__).resolve().parent
RAW_DATA_DIR: Path = PROJECT_ROOT / "raw_data"
LOGS_DIR: Path = PROJECT_ROOT / "logs"


def calculate_historical_date_range(
    reference_date: datetime | None = None,
    historical_days: int = HISTORICAL_DAYS,
    buffer_days: int = AVAILABILITY_BUFFER_DAYS,
) -> Tuple[str, str]:
    """
    Calculates dynamic start and end dates in YYYY-MM-DD format normalized to UTC.

    The range is inclusive and contains exactly `historical_days` calendar dates.
    An availability buffer accounts for the lag in finalizing ERA5 reanalysis data.

    Args:
        reference_date: Base datetime. If naive, assumed UTC; if timezone-aware,
            normalized to UTC. Defaults to current UTC datetime.
        historical_days: Number of calendar days to extract (must be > 0).
        buffer_days: Lag days subtracted from current date (must be >= 0).

    Returns:
        Tuple of (start_date_str, end_date_str) in ISO-8601 YYYY-MM-DD format.

    Raises:
        ValueError: If historical_days <= 0 or buffer_days < 0.
    """
    if historical_days <= 0:
        raise ValueError(f"historical_days must be greater than 0, got {historical_days}")
    if buffer_days < 0:
        raise ValueError(f"buffer_days must be non-negative (>= 0), got {buffer_days}")

    if reference_date is None:
        normalized_date = datetime.now(timezone.utc)
    elif reference_date.tzinfo is None:
        normalized_date = reference_date.replace(tzinfo=timezone.utc)
    else:
        normalized_date = reference_date.astimezone(timezone.utc)

    # End date is buffer_days before the normalized UTC reference date
    end_date = (normalized_date - timedelta(days=buffer_days)).date()
    # Start date ensures exactly `historical_days` inclusive: end - (historical_days - 1)
    start_date = end_date - timedelta(days=historical_days - 1)

    return start_date.isoformat(), end_date.isoformat()
