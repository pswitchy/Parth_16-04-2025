This service monitors restaurant store uptime based on polling data, business hours, and timezones. It provides an API to trigger report generation and retrieve the results, utilizing multiprocessing for optimized report calculation.

## Features

*   Loads store status, business hours, and timezone data from CSVs into a database (SQLite or PostgreSQL). Handles UUIDs for `store_id`.
*   Calculates store uptime and downtime for the last hour, day, and week, considering only business hours.
*   Uses interpolation based on status polls to estimate uptime/downtime for the entire business hour intervals.
*   Handles timezones correctly using `pytz` and includes caching for timezone objects.
*   Provides a REST API:
    *   `/trigger_report`: Starts report generation in the background using **multiprocessing**.
    *   `/get_report/{report_id}`: Polls for report status or downloads the completed CSV report.
*   Default Handling: Assumes stores are open 24/7 if business hours are missing and uses 'America/Chicago' if timezone is missing.

## API Usage

1.  **Trigger Report Generation:**
    *   Send a POST request to `/trigger_report`.
    *   Example using `curl`:
        ```bash
        curl -X POST http://127.0.0.1:8000/trigger_report
        ```
    *   The response will contain a `report_id`:
        ```json
        {
          "report_id": "some-unique-uuid-string"
        }
        ```

2.  **Get Report Status / Download CSV:**
    *   Send a GET request to `/get_report/{report_id}`, replacing `{report_id}` with the ID received.
    *   Example using `curl`:
        ```bash
        curl http://127.0.0.1:8000/get_report/some-unique-uuid-string
        ```
    *   **If Running:** Response `{"status": "Running"}`.
    *   **If Failed:** Response `{"status": "Failed"}`.
    *   **If Complete:** The response will be the CSV file content. Save it:
        ```bash
        curl http://127.0.0.1:8000/get_report/some-unique-uuid-string -o report_output.csv
        ```
    *   Generated CSV files are also stored in the `reports/` directory.

## Design Notes & Logic

*   **Data Model:** Uses SQLAlchemy ORM with `String` type for `store_id` to accommodate UUIDs.
*   **Timestamp:** The "current time" for report calculation is determined by the maximum timestamp found in the `store_status` table at the start of report generation.
*   **Interpolation:** Assumes store status remains constant between polls. The last poll's status extends to the "current time".
*   **Business Hours:** Local business hours are converted to UTC intervals *for each specific date* within the reporting period (last week), handling timezones (`pytz`) and DST. 24/7 is the default if hours are missing.
*   **Timezones:** Fetched from the DB or defaults to 'America/Chicago'. `pytz` objects are cached using `lru_cache`.
*   **Uptime/Downtime:** Calculated by finding the intersection between interpolated status intervals and the calculated UTC business hour intervals for each day. Durations are summed for the last hour/day/week.
*   **Optimization:** Report generation is parallelized using Python's `multiprocessing.Pool` within a FastAPI `BackgroundTasks` function. Each worker process calculates the report for a subset of stores, improving performance on multi-core systems.
*   **Data Loading:** The `load_data.py` script handles loading from CSVs, including basic validation, parsing, and handling of missing/default values. It uses UPSERT for timezones and DELETE/INSERT for business hours.

## Example Output CSV Link

https://drive.google.com/file/d/1Jsmhm_xXQFYhHMYkzEkfYOzgInyhcLZY/view?usp=drive_link

## Potential Improvements / Production Considerations

1.  **Dedicated Task Queue:** For enhanced scalability, reliability (retries, persistence), and monitoring in production, replace FastAPI's `BackgroundTasks` + `multiprocessing` with a dedicated system like **Celery** + **Redis/RabbitMQ**. This decouples task processing from the API server.
2.  **Async Database:** Use `asyncpg` and SQLAlchemy's async support for non-blocking database operations, improving API concurrency, especially under high load or when combined with an async task queue.
3.  **Database Tuning:** Ensure optimal indexing, consider connection pool tuning (especially with multiprocessing/Celery), and potentially more advanced query optimization for very large datasets.

## CLI Output

![loop ai_output](https://github.com/user-attachments/assets/5a702139-e46e-4075-b117-63ccc7b63cfe)
