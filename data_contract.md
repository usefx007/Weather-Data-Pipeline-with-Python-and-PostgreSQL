# Data Contract: Weather Data Pipeline — Extraction Layer (Mohamed Mahmoud)

**Version:** 1.0.1  
**Effective Date:** 2026-11-25  
**Data Producer:** Mohamed Mahmoud — Data Extraction (Backend)
**Data Consumers:**  
- **Seif Moataz — Data Cleaning & Transformation (Pandas / ETL)**
- **Youssef Ali — Database Schema & Ingestion (PostgreSQL)**
- **Bedeir Abdelhamid — Advanced SQL & Analytics**
- **Mohamed Mosad — Workflow Orchestration (Apache Airflow)**
- **Ibrahim Mohamed — Containerization & DevOps (Docker)**

---

## 1. Executive Summary & Purpose

This contract formally defines the schema, storage conventions, data types, validation guarantees, and handling policies for the raw historical weather datasets produced by **Mohamed Mahmoud (Data Extraction)**.

All downstream consumers (**Seif Moataz** for Pandas transformation and **Youssef Ali** for PostgreSQL staging) must build their pipelines against this specification. Any breaking schema change requires a major version bump in this contract.

---

## 2. Storage & Directory Conventions

### 2.1 File Path Structure
Raw weather files are organized per city in dedicated directories under `Member 1 — Data Extraction/raw_data/`:

```text
Member 1 — Data Extraction/raw_data/
├── alexandria/
│   └── weather_alexandria_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS_microseconds_uuid8.json
├── aswan/
│   └── weather_aswan_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS_microseconds_uuid8.json
├── cairo/
│   └── weather_cairo_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS_microseconds_uuid8.json
├── mansoura/
│   └── weather_mansoura_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS_microseconds_uuid8.json
└── port_said/
    └── weather_port_said_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSS_microseconds_uuid8.json
```

### 2.2 Filename Convention
Each file follows a strict naming format designed to minimize collision risk:
```text
weather_{city_key}_{start_date}_{end_date}_{timestamp_utc}_{uuid8}.json
```
- `{city_key}`: Lowercase normalized identifier (`cairo`, `alexandria`, `mansoura`, `aswan`, `port_said`).
- `{start_date}`: Inclusive start date in ISO-8601 (`YYYY-MM-DD`).
- `{end_date}`: Inclusive end date in ISO-8601 (`YYYY-MM-DD`).
- `{timestamp_utc}`: UTC extraction timestamp (`YYYYMMDD_HHMMSS_microseconds`).
- `{uuid8}`: First 8 characters of a UUIDv4 token to significantly reduce filename collision risk across concurrent or rapid successive runs.

### 2.3 Storage Invariants
1. **Immutability (Operational Policy):** As an operational policy, the extraction pipeline strictly treats raw data as write-once; it never modifies, overwrites, or deletes previously saved raw JSON files in-place.
2. **Atomicity:** All writes use temporary files (`.tmp_*.json`) in the target directory and atomic rename (`temp_path.replace(final_path)`). If an extraction or write fails mid-stream, partial files are cleanly scrubbed and no final file is created.
3. **Extraction History:** Consecutive extraction runs generate distinct files stamped with UTC timestamps and unique tokens, preserving complete extraction lineage and execution history.

---

## 3. Data Envelope Schema

Every raw JSON file contains an outer wrapper separating extraction metadata from the unaltered upstream API payload:

```json
{
  "metadata": {
    "city": "Cairo",
    "city_key": "cairo",
    "latitude": 30.0444,
    "longitude": 31.2357,
    "source": "Open-Meteo Historical Weather API",
    "extracted_at_utc": "2026-11-24T21:47:36Z",
    "requested_start_date": "2026-10-21",
    "requested_end_date": "2026-11-19",
    "request_parameters": {
      "latitude": 30.0444,
      "longitude": 31.2357,
      "start_date": "2026-10-21",
      "end_date": "2026-11-19",
      "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code",
      "temperature_unit": "celsius",
      "wind_speed_unit": "kmh",
      "precipitation_unit": "mm",
      "timezone": "UTC"
    }
  },
  "original_api_response": {
    "latitude": 30.052723,
    "longitude": 31.190199,
    "generationtime_ms": 2.2438,
    "utc_offset_seconds": 0,
    "timezone": "GMT",
    "timezone_abbreviation": "GMT",
    "elevation": 22.0,
    "hourly_units": {
      "time": "iso8601",
      "temperature_2m": "°C",
      "relative_humidity_2m": "%",
      "precipitation": "mm",
      "wind_speed_10m": "km/h",
      "weather_code": "wmo code"
    },
    "hourly": {
      "time": ["2026-10-21T00:00", "..."],
      "temperature_2m": [22.4, "..."],
      "relative_humidity_2m": [65, "..."],
      "precipitation": [0.0, "..."],
      "wind_speed_10m": [12.2, "..."],
      "weather_code": [0, "..."]
    }
  }
}
```

---

## 4. Field Specification & Data Types

### 4.1 Metadata Layer (`metadata`)

| Field Name | Type | Constraints | Description |
|---|---|---|---|
| `city` | `string` | Non-empty | Official display name (e.g., `"Cairo"`, `"Alexandria"`). |
| `city_key` | `string` | Lowercase snake_case | Normalized lookup key matching directory name. |
| `latitude` | `float` | -90.0 to +90.0 | Requested geographic latitude coordinate. |
| `longitude` | `float` | -180.0 to +180.0 | Requested geographic longitude coordinate. |
| `source` | `string` | Constant | `"Open-Meteo Historical Weather API"`. |
| `extracted_at_utc` | `string` | ISO-8601 (`YYYY-MM-DDTHH:MM:SSZ`) | Exact timestamp when data was fetched by pipeline. |
| `requested_start_date`| `string` | ISO-8601 (`YYYY-MM-DD`) | Earliest date requested (inclusive). |
| `requested_end_date` | `string` | ISO-8601 (`YYYY-MM-DD`) | Latest date requested (inclusive). |
| `request_parameters` | `object` | Key-value pairs | Exact HTTP query parameters sent to Open-Meteo. |

### 4.2 Upstream Response Layer (`original_api_response`)

| Field Name | Type | Example | Description |
|---|---|---|---|
| `latitude` | `float` | `30.0527` | Actual grid-cell center latitude resolved by ERA5. |
| `longitude` | `float` | `31.1902` | Actual grid-cell center longitude resolved by ERA5. |
| `generationtime_ms`| `float` | `2.24` | Upstream API query processing time in milliseconds. |
| `utc_offset_seconds`| `integer` | `0` | Offset relative to UTC (strictly normalized to 0). |
| `timezone` | `string` | `"GMT"` | Resolved timezone abbreviation. |
| `elevation` | `float` | `22.0` | Elevation of geographic location above sea level in meters. |
| `hourly_units` | `object` | See §4.3 | Dictionary documenting physical measurement units. |
| `hourly` | `object` | See §4.3 | Dictionary of parallel hourly measurement arrays. |

### 4.3 Hourly Arrays Specification (`original_api_response.hourly`)

All arrays inside `hourly` are **parallel** and contain exactly the same number of elements ($N = 24 \times \text{days} = 720$ elements for a 30-day window):

| Field Name | Physical Unit | JSON Type | Allowed Values / Ranges | Nullable? | Description |
|---|---|---|---|---|---|
| `time` | ISO-8601 (`YYYY-MM-DDTHH:MM`) | `string` | Ascending hourly timestamps | **NO** (Strictly non-null) | Hourly point in time. |
| `temperature_2m` | Celsius (`°C`) | `number` | Typical: `-10.0` to `60.0` | Yes (Preserved) | Air temperature at 2 meters above ground. |
| `relative_humidity_2m` | Percentage (`%`) | `number` | `0.0` to `100.0` | Yes (Preserved) | Relative air humidity at 2 meters. |
| `precipitation` | Millimeters (`mm`) | `number` | $\ge 0.0$ | Yes (Preserved) | Total precipitation (rain/showers/snow) per hour. |
| `wind_speed_10m` | Kilometers/hr (`km/h`)| `number` | $\ge 0.0$ | Yes (Preserved) | Wind speed at 10 meters above ground level. |
| `weather_code` | WMO Code | `integer` | `0` to `99` (See §5) | Yes (Preserved) | World Meteorological Organization weather code. |

---

## 5. Reference: WMO Weather Interpretation Codes

Seif Moataz (Pandas Cleaning) and Youssef Ali (PostgreSQL) should use this mapping table when transforming `weather_code`:

| Code Range | Meaning | Description |
|---|---|---|
| `0` | Clear sky | Cloudless conditions. |
| `1, 2, 3` | Mainly clear, partly cloudy, overcast | Increasing cloud cover. |
| `45, 48` | Fog and depositing rime fog | Reduced visibility. |
| `51, 53, 55` | Drizzle: Light, moderate, and dense intensity | Continuous light precipitation. |
| `56, 57` | Freezing Drizzle: Light and dense intensity | Freezing sub-zero precipitation. |
| `61, 63, 65` | Rain: Slight, moderate, and heavy intensity | Standard liquid rainfall. |
| `66, 67` | Freezing Rain: Light and heavy | Supercooled rain droplets. |
| `71, 73, 75` | Snow fall: Slight, moderate, and heavy | Solid frozen precipitation. |
| `77` | Snow grains | Fine ice particles. |
| `80, 81, 82` | Rain showers: Slight, moderate, and violent | Convective intermittent precipitation. |
| `85, 86` | Snow showers: Slight and heavy | Intermittent snow. |
| `95` | Thunderstorm: Slight or moderate | Lightning / thunder. |
| `96, 99` | Thunderstorm with slight and heavy hail | Severe convective storms. |

---

## 6. Guarantees & Handover SLA

Mohamed Mahmoud guarantees the following Service Level Agreements (SLAs) for every file produced and saved in `Member 1 — Data Extraction/raw_data/`:

1. **Parallel Array Length Guarantee:**
   - For every successfully saved file, `len(hourly["time"]) == len(hourly[variable])` for all hourly variables.
   - Any API response where array lengths mismatch is rejected by the validator and aborted before writing to disk.
2. **Missing Value Preservation Policy:**
   - Mohamed Mahmoud **never drops, filters, or artificially imputes** missing or `null` values.
   - Legitimate upstream `null` values are strictly preserved as JSON `null` so that Seif Moataz can implement appropriate domain-specific data cleaning (e.g., forward-fill, interpolation, or sentinel flagging).
3. **Availability Buffer Implementation:**
   - Extraction incorporates a 5-day availability buffer (`AVAILABILITY_BUFFER_DAYS = 5`) to account for the publishing and finalization delay of ECMWF ERA5 reanalysis data.
   - **Note:** This buffer aims to avoid requesting unavailable dates from the archive, but does *not* constitute a guarantee that upstream meteorological sensors will never report missing or `null` readings.
4. **City Failure Isolation:**
   - If an extraction or network error occurs for a specific city, remaining cities are processed independently.
   - A successful batch run produces files only for cities whose extraction and validation succeeded.
5. **Atomic Storage Guarantee:**
   - Files are written to temporary files and atomically renamed. In case of an unhandled write exception, temporary files are cleaned up and no corrupt partial file remains.
6. **Character Encoding:**
   - All files are UTF-8 encoded with standard JSON formatting (`ensure_ascii=False`, `indent=2`).

---

## 7. Instructions for Downstream Consumers

### For Seif Moataz (Data Cleaning & Transformation):
- Ingest files by iterating over `Member 1 — Data Extraction/raw_data/*/*.json`.
- Extract `data["metadata"]` to enrich the DataFrame with `city_name`, `city_key`, and `extracted_at_utc`.
- Convert `data["original_api_response"]["hourly"]` directly into a `pandas.DataFrame(hourly)`.
- Parse timestamps explicitly using `pd.to_datetime(hourly["time"], utc=True)`.
- Inspect and handle any upstream `null` values in sensor fields (temperature, humidity, precipitation, wind speed, weather code) according to project cleaning rules.
- **Multiple extraction snapshots:** A city folder may contain multiple files for the same requested date range. Do not concatenate these files blindly. Treat them as separate extraction snapshots, compare `metadata.extracted_at_utc` and their hourly payloads, then select the intended snapshot or apply a documented deduplication policy using `(city_key, time)`. Preserve extraction lineage and do not delete source files during this process.

### For Youssef Ali (PostgreSQL Database Schema & Ingestion):
- Proposed staging table schema:
  ```sql
  CREATE TABLE staging_raw_weather (
      id SERIAL PRIMARY KEY,
      city VARCHAR(50) NOT NULL,
      city_key VARCHAR(50) NOT NULL,
      recorded_at TIMESTAMPTZ NOT NULL,
      temperature_celsius NUMERIC(5, 2),
      relative_humidity_pct NUMERIC(5, 2),
      precipitation_mm NUMERIC(6, 2),
      wind_speed_kmh NUMERIC(5, 2),
      weather_code INT,
      extracted_at TIMESTAMPTZ NOT NULL,
      CONSTRAINT uq_city_time UNIQUE (city_key, recorded_at)
  );
  ```
