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

## Potential Improvements

1.  **Dedicated Task Queue:** For enhanced scalability, reliability (retries, persistence), and monitoring in production, I can replace FastAPI's `BackgroundTasks` + `multiprocessing` with a dedicated system like **Celery** + **Redis/RabbitMQ**. This decouples task processing from the API server.
2. **Business Hour Calculation Optimization**: get_utc_business_intervals is called for every day within potentially long status intervals. For a week-long report, this might be called 7 times per status interval. It could potentially be optimized by calculating the UTC intervals for the entire week once per store at the beginning, creating a data structure (like a sorted list of UTC intervals) representing the full week's business hours. Then, the intersection logic would compare the status interval against this pre-calculated weekly schedule. This trades off potentially higher initial memory use for fewer repeated timezone conversions.
3. **Handling Sparse Data**: The interpolation assumes status is constant between polls. If polls are very infrequent (e.g., many hours apart), the calculated uptime/downtime might not accurately reflect reality. The logic itself is sound based on the assumption, but the quality of the result depends on poll frequency. This isn't strictly a logic flaw, but an inherent limitation based on the input data's nature. I could add logging if intervals are excessively long.
4. **"Current Time" Definition**: Using the max timestamp from the store_status table is reasonable, but if data loading lags, the report might be significantly behind real-time. An alternative could be to use datetime.now(pytz.utc) or allow passing a specific end-time parameter to the report trigger.

## CLI Output

![loop ai_output](https://github.com/user-attachments/assets/5a702139-e46e-4075-b117-63ccc7b63cfe)
