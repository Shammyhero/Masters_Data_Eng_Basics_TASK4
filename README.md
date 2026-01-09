# Restaurant & Weather Data Pipeline

This project implements a local PySpark data pipeline that cleans restaurant data, enriches missing coordinates using the OpenCage Geocoding API, generates geohashes, and joins the data with weather information.

## 📂 Project Structure

```
project/
 ├── src/
 │    └── job.py            # Main PySpark ETL job (includes geocoding & geohash logic)
 ├── tests/
 │    ├── test_geohash.py   # Unit tests for geohashing
 │    ├── test_join.py      # Integration tests for Spark logic
 │    └── test_geocoding.py # Tests for API client (mocked)
 ├── data/
 │    ├── restaurants/      # Input CSV files
 │    └── weather/          # Input Parquet files (partitioned)
 └── requirements.txt
```

## 🚀 How to Run

### 1. Prerequisites
*   **Python 3.10+**
*   **Java 8, 11, or 17** (Required for PySpark)
*   **OpenCage API Key**: [Get one here](https://opencagedata.com/)

### 2. Setup Environment
Install the required Python packages:
```bash
pip install -r requirements.txt
```

### 3. Configure API Key
Create a `.env` file in the project root directory and add your OpenCage API Key:
```bash
OPENCAGE_API_KEY=your_actual_api_key_here
```

### 4. Prepare Data
Ensure your input data is present:
*   **Restaurants**: CSV format in `data/restaurants/`. Expected columns: `id`, `franchise_name`, `city`, `country`, `lat`, `lng`.
*   **Weather**: Parquet format (partitioned) in `data/weather/`. Expected columns: `lat`, `lng`, `avg_tmpr_c`, `wthr_date`.

### 5. Run the Job
Execute the main ETL script:
```bash
python3 src/job.py
```
The job will:
1.  Read restaurants (CSV) and weather (Parquet).
2.  Enrich missing restaurant coordinates via API (using City/Country).
3.  Generate geohashes for both datasets.
4.  Deduplicate weather data (latest date per geohash).
5.  Join datasets on geohash.
6.  Write the result to `output/enriched_data` (Parquet format).

### 6. Run Tests
Run the test suite to verify logic (mocking the API):
```bash
python3 -m pytest tests/
```
