"""
Weather Data Extraction Module (Member 1 - Data Extraction).

Extracts historical weather observations from the Open-Meteo Historical Weather API
for multiple target cities, applies resilient retry logic with exponential backoff,
validates payload structure, and stores raw JSON responses with metadata wrappers.
"""

import json
import logging
from logging.handlers import RotatingFileHandler
import sys
import time
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

import config

# Configure module-level logger
logger = logging.getLogger("weather_extractor")


def setup_logging(
    log_to_file: bool = True,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """
    Configures application logging to console and a rotating file handler.

    Args:
        log_to_file: If True, writes logs to a rotating file in config.LOGS_DIR.
        max_bytes: Maximum size of a log file before rotation (default 5MB).
        backup_count: Number of rotated backup log files to retain.

    Returns:
        Configured logger instance.
    """
    logger.setLevel(logging.INFO)
    for handler in logger.handlers:
        try:
            handler.close()
        except Exception:
            pass
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console Stream Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Rotating File Handler
    if log_to_file:
        config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        log_file_path = config.LOGS_DIR / "extraction.log"
        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def build_api_parameters(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    hourly_variables: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Constructs query parameters for the Open-Meteo Historical API request.

    Args:
        latitude: Geographic latitude in decimal degrees.
        longitude: Geographic longitude in decimal degrees.
        start_date: Inclusive start date (YYYY-MM-DD).
        end_date: Inclusive end date (YYYY-MM-DD).
        hourly_variables: List of hourly weather variable names.

    Returns:
        Dictionary of request query parameters.
    """
    variables = hourly_variables or config.HOURLY_VARIABLES
    return {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(variables),
        "temperature_unit": config.TEMPERATURE_UNIT,
        "wind_speed_unit": config.WIND_SPEED_UNIT,
        "precipitation_unit": config.PRECIPITATION_UNIT,
        "timezone": "UTC",
    }


def parse_retry_after(retry_after_val: Optional[str], default_wait: float) -> float:
    """
    Parses HTTP 'Retry-After' header supporting both integer seconds and HTTP-date formats.

    Args:
        retry_after_val: Raw string from response header (seconds or HTTP date).
        default_wait: Fallback wait time in seconds if header is missing or unparseable.

    Returns:
        Non-negative wait time in seconds.
    """
    if not retry_after_val:
        return default_wait

    cleaned = retry_after_val.strip()

    # Form 1: Integer delay in seconds (e.g., '120')
    if cleaned.isdigit():
        return max(0.0, float(cleaned))

    # Form 2: HTTP-date format (e.g., 'Wed, 21 Oct 2026 07:28:00 GMT')
    try:
        target_time = parsedate_to_datetime(cleaned)
        now_utc = datetime.now(timezone.utc)
        diff_seconds = (target_time - now_utc).total_seconds()
        return max(0.0, diff_seconds)
    except Exception:
        return default_wait


def fetch_weather_data(
    session: requests.Session,
    params: Dict[str, Any],
    city_name: str,
    max_retries: int = config.MAX_RETRIES,
    initial_backoff: float = config.INITIAL_BACKOFF_SECONDS,
    backoff_factor: float = config.BACKOFF_FACTOR,
    timeout: int = config.REQUEST_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """
    Sends an HTTP GET request to the Open-Meteo API with retry and exponential backoff.

    Behavior:
        - HTTP 429: Respects 'Retry-After' (numeric seconds or HTTP-date).
        - HTTP 500, 502, 503, 504: Retries with exponential backoff.
        - Network errors (Timeout, ConnectionError): Retries with exponential backoff.
        - HTTP 4xx (Client Errors, e.g., 400, 404): Fails immediately without retries.
        - Does NOT sleep after the final attempt fails.

    Args:
        session: Active requests.Session instance.
        params: API query parameters.
        city_name: Name of the city for logging.
        max_retries: Maximum number of attempts.
        initial_backoff: Initial delay in seconds.
        backoff_factor: Multiplier for exponential delay.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response dictionary from API.

    Raises:
        requests.RequestException: If network or server error persists across all retries,
            or if a permanent client error occurs.
        ValueError: If response body is not valid JSON.
    """
    if max_retries < 1:
        raise ValueError(f"max_retries must be at least 1, got {max_retries}")
    if initial_backoff < 0:
        raise ValueError(f"initial_backoff must be non-negative (>= 0), got {initial_backoff}")
    if backoff_factor < 1:
        raise ValueError(f"backoff_factor must be at least 1, got {backoff_factor}")

    attempt = 0
    last_error: Optional[Exception] = None

    while attempt < max_retries:
        attempt += 1
        logger.info(f"[{city_name}] Requesting weather data (Attempt {attempt}/{max_retries})...")

        try:
            response = session.get(
                config.API_BASE_URL,
                params=params,
                timeout=timeout,
            )

            # Permanent Client Errors (4xx except 429) -> Fail fast immediately
            if 400 <= response.status_code < 500 and response.status_code != 429:
                logger.error(
                    f"[{city_name}] Permanent client error (HTTP {response.status_code}): {response.text}"
                )
                response.raise_for_status()

            # Rate Limiting (HTTP 429) -> Retryable
            if response.status_code == 429:
                default_delay = initial_backoff * (backoff_factor ** (attempt - 1))
                wait_seconds = parse_retry_after(response.headers.get("Retry-After"), default_delay)
                logger.warning(
                    f"[{city_name}] Rate limited (HTTP 429). Retry-After evaluated to {wait_seconds:.1f}s."
                )

                if attempt < max_retries:
                    time.sleep(wait_seconds)
                    continue
                else:
                    response.raise_for_status()

            # Transient Server Errors (HTTP 500, 502, 503, 504) -> Retryable
            if response.status_code in {500, 502, 503, 504}:
                wait_seconds = initial_backoff * (backoff_factor ** (attempt - 1))
                logger.warning(
                    f"[{city_name}] Transient server error (HTTP {response.status_code}). "
                    f"Backoff delay {wait_seconds:.1f}s."
                )

                if attempt < max_retries:
                    time.sleep(wait_seconds)
                    continue
                else:
                    response.raise_for_status()

            # Any other unexpected non-200 status
            response.raise_for_status()

            # Safe JSON parsing
            try:
                data = response.json()
                logger.info(f"[{city_name}] Successfully received valid response.")
                return data
            except ValueError as json_err:
                logger.error(f"[{city_name}] Invalid JSON response received: {json_err}")
                raise ValueError(f"Malformed JSON from API for {city_name}: {json_err}") from json_err

        except (requests.ConnectionError, requests.Timeout) as net_err:
            last_error = net_err
            wait_seconds = initial_backoff * (backoff_factor ** (attempt - 1))
            logger.warning(
                f"[{city_name}] Network error ({net_err.__class__.__name__}) on attempt {attempt}: {net_err}."
            )

            if attempt < max_retries:
                time.sleep(wait_seconds)
            else:
                logger.error(f"[{city_name}] Exceeded max retries ({max_retries}). Extraction failed.")
                raise net_err

        except requests.HTTPError as http_err:
            last_error = http_err
            # If it's a permanent 4xx or last attempt of retryable error, re-raise immediately
            raise http_err

    if last_error:
        raise last_error
    raise requests.RequestException(f"[{city_name}] Failed to extract data after {max_retries} attempts.")


def validate_weather_payload(payload: Any, expected_variables: Optional[List[str]] = None) -> bool:
    """
    Validates structural integrity of the Open-Meteo API response.

    Checks:
        1. Payload is a valid non-empty dictionary.
        2. 'hourly' section exists and is a dictionary.
        3. 'time' array exists in hourly data.
        4. All requested weather variable arrays exist.
        5. All hourly arrays (time + variables) have equal non-zero lengths.

    Note: Legitimate missing observations (None/null inside arrays) are intentionally
    preserved for Member 2 (Data Cleaning) and do NOT invalidate the response.

    Args:
        payload: Decoded API response object.
        expected_variables: List of variable names expected under 'hourly'.

    Returns:
        True if validation passes.

    Raises:
        ValueError: If any structural validation check fails.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object, got {type(payload).__name__}")

    if "hourly" not in payload or not isinstance(payload["hourly"], dict):
        raise ValueError("Response missing required 'hourly' object.")

    hourly = payload["hourly"]

    if "time" not in hourly or not isinstance(hourly["time"], list):
        raise ValueError("Hourly section missing 'time' array.")

    expected_vars = expected_variables or config.HOURLY_VARIABLES
    for var in expected_vars:
        if var not in hourly:
            raise ValueError(f"Hourly section missing expected variable '{var}'.")
        if not isinstance(hourly[var], list):
            raise ValueError(f"Hourly variable '{var}' must be a list.")

    # Consistency check: all arrays must have identical length
    expected_length = len(hourly["time"])
    if expected_length == 0:
        raise ValueError("Hourly 'time' array is empty. No observations returned.")

    for var in expected_vars:
        var_length = len(hourly[var])
        if var_length != expected_length:
            raise ValueError(
                f"Array length mismatch: 'time' has {expected_length} items, "
                f"but '{var}' has {var_length} items."
            )

    return True


def save_raw_weather_data(
    city_key: str,
    original_api_response: Dict[str, Any],
    request_params: Dict[str, Any],
    start_date: str,
    end_date: str,
    city_metadata: Optional[Dict[str, Any]] = None,
    base_dir: Optional[Path] = None,
) -> Path:
    """
    Saves original API response encapsulated in a metadata wrapper.

    Safety & Reliability Features:
        - Resolves city metadata from passed argument, config.CITIES, or derives fallback.
        - Generates collision-resistant filenames using microsecond timestamps and random UUIDs.
        - Performs atomic file writes (temporary file renamed to destination).
        - Cleans up temporary files in a finally block if write fails.

    Args:
        city_key: City identifier (e.g., 'cairo').
        original_api_response: Unaltered API response payload.
        request_params: Parameters used in the HTTP request.
        start_date: Requested start date (YYYY-MM-DD).
        end_date: Requested end date (YYYY-MM-DD).
        city_metadata: Optional city config dict with 'display_name', 'latitude', 'longitude', 'folder_name'.
        base_dir: Base directory for storage (defaults to config.RAW_DATA_DIR).

    Returns:
        Path to the saved JSON file.
    """
    # Resolve city configuration safely
    if city_metadata:
        resolved_city = city_metadata
    elif city_key in config.CITIES:
        resolved_city = config.CITIES[city_key]
    else:
        resolved_city = {
            "display_name": city_key.replace("_", " ").title(),
            "latitude": request_params.get("latitude", 0.0),
            "longitude": request_params.get("longitude", 0.0),
            "folder_name": city_key.lower().replace(" ", "_"),
        }

    city_folder_name = resolved_city.get("folder_name", city_key.lower())
    target_dir = (base_dir or config.RAW_DATA_DIR) / city_folder_name
    target_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    extracted_at_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    file_timestamp = now_utc.strftime("%Y%m%d_%H%M%S_%f")
    unique_suffix = uuid.uuid4().hex[:8]

    # Collision-resistant filename: weather_<city>_<start>_<end>_<timestamp_us>_<uuid>.json
    filename = f"weather_{city_folder_name}_{start_date}_{end_date}_{file_timestamp}_{unique_suffix}.json"
    final_path = target_dir / filename
    temp_path = target_dir / f".tmp_{filename}"

    wrapper = {
        "metadata": {
            "city": resolved_city.get("display_name", city_key),
            "city_key": city_key,
            "latitude": resolved_city.get("latitude", request_params.get("latitude")),
            "longitude": resolved_city.get("longitude", request_params.get("longitude")),
            "source": "Open-Meteo Historical Weather API",
            "extracted_at_utc": extracted_at_utc,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "request_parameters": request_params,
        },
        "original_api_response": original_api_response,
    }

    try:
        # Atomic write
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(wrapper, f, indent=2, ensure_ascii=False)
        temp_path.replace(final_path)
    finally:
        # Ensure temporary file cleanup on any failure
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass

    logger.info(f"[{resolved_city.get('display_name', city_key)}] Raw data saved to: {final_path}")
    return final_path


def extract_city(
    city_key: str,
    session: requests.Session,
    start_date: str,
    end_date: str,
    city_metadata: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, Optional[Path], Optional[str]]:
    """
    Executes the extraction pipeline for a single city.

    Args:
        city_key: City identifier.
        session: Active requests.Session.
        start_date: ISO start date (YYYY-MM-DD).
        end_date: ISO end date (YYYY-MM-DD).
        city_metadata: Optional city config override.

    Returns:
        Tuple of (success_status, saved_file_path_or_none, error_message_or_none).
    """
    if city_metadata:
        resolved_city = city_metadata
    elif city_key in config.CITIES:
        resolved_city = config.CITIES[city_key]
    else:
        resolved_city = {
            "display_name": city_key.replace("_", " ").title(),
            "latitude": 0.0,
            "longitude": 0.0,
            "folder_name": city_key.lower().replace(" ", "_"),
        }

    city_name = resolved_city["display_name"]

    try:
        try:
            parsed_start = datetime.strptime(str(start_date), "%Y-%m-%d").date()
            parsed_end = datetime.strptime(str(end_date), "%Y-%m-%d").date()
        except ValueError as parse_err:
            raise ValueError(
                f"Invalid date format (expected YYYY-MM-DD): {parse_err}"
            ) from parse_err

        if parsed_start > parsed_end:
            raise ValueError(
                f"Invalid date range: start_date ({start_date}) cannot be after end_date ({end_date})."
            )

        params = build_api_parameters(
            latitude=resolved_city["latitude"],
            longitude=resolved_city["longitude"],
            start_date=start_date,
            end_date=end_date,
        )

        data = fetch_weather_data(session=session, params=params, city_name=city_name)
        validate_weather_payload(data)
        saved_file = save_raw_weather_data(
            city_key=city_key,
            original_api_response=data,
            request_params=params,
            start_date=start_date,
            end_date=end_date,
            city_metadata=resolved_city,
        )
        return True, saved_file, None

    except Exception as err:
        logger.error(f"[{city_name}] Extraction failed: {err}")
        return False, None, str(err)


def extract_all_cities(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    target_cities: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Main extraction orchestrator: processes all target cities with failure isolation.

    Validates date ranges strictly: rejects partial date specifications.
    Callable by Member 5 (Airflow DAG) or CLI execution.

    Args:
        start_date: Optional explicit start date (YYYY-MM-DD).
        end_date: Optional explicit end date (YYYY-MM-DD).
        target_cities: Dictionary of cities to extract (defaults to config.CITIES).

    Returns:
        Structured execution summary dictionary.

    Raises:
        ValueError: If only one of start_date or end_date is provided, or if dates are invalid.
    """
    cities_to_process = target_cities if target_cities is not None else config.CITIES

    # Validate date input pairing
    if bool(start_date) != bool(end_date):
        raise ValueError(
            "Incomplete date range provided: both start_date and end_date must be specified together, "
            "or both omitted to use the default historical window."
        )

    # Calculate dynamic dates if both are omitted
    if not start_date and not end_date:
        calc_start, calc_end = config.calculate_historical_date_range()
        start_date = calc_start
        end_date = calc_end
    else:
        # Validate format and chronological order when explicitly provided
        try:
            parsed_start = datetime.strptime(str(start_date), "%Y-%m-%d").date()
            parsed_end = datetime.strptime(str(end_date), "%Y-%m-%d").date()
        except ValueError as parse_err:
            raise ValueError(
                f"Invalid date format (expected YYYY-MM-DD): {parse_err}"
            ) from parse_err

        if parsed_start > parsed_end:
            raise ValueError(
                f"Invalid date range: start_date ({start_date}) cannot be after end_date ({end_date})."
            )

    logger.info("=" * 60)
    logger.info("Starting Weather Data Extraction Pipeline (Member 1)")
    logger.info(f"Target Date Range: {start_date} to {end_date}")
    logger.info(f"Cities to process: {list(cities_to_process.keys())}")
    logger.info("=" * 60)

    summary: Dict[str, Any] = {
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "successful_cities": [],
        "failed_cities": [],
        "saved_files": [],
        "errors": {},
        "total_cities": len(cities_to_process),
        "is_fully_successful": False,
    }

    with requests.Session() as session:
        for city_key, city_info in cities_to_process.items():
            city_name = city_info.get("display_name", city_key)
            success, saved_path, error_msg = extract_city(
                city_key=city_key,
                session=session,
                start_date=start_date,
                end_date=end_date,
                city_metadata=city_info,
            )

            if success and saved_path:
                summary["successful_cities"].append(city_name)
                summary["saved_files"].append(str(saved_path))
            else:
                summary["failed_cities"].append(city_name)
                summary["errors"][city_name] = error_msg

    summary["is_fully_successful"] = (len(summary["failed_cities"]) == 0)

    logger.info("-" * 60)
    logger.info("Extraction Run Finished Summary:")
    logger.info(f"Successful ({len(summary['successful_cities'])}/{summary['total_cities']}): {summary['successful_cities']}")
    if summary["failed_cities"]:
        logger.warning(f"Failed ({len(summary['failed_cities'])}/{summary['total_cities']}): {summary['failed_cities']}")
    logger.info(f"Saved {len(summary['saved_files'])} raw data files.")
    logger.info("=" * 60)

    return summary


def main() -> None:
    """Entry point for command line execution."""
    setup_logging()
    result = extract_all_cities()
    if not result["is_fully_successful"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
