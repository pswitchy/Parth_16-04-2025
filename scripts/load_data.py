# scripts/load_data.py
import pandas as pd
import os
import sys
from sqlalchemy.orm import Session
from datetime import datetime, time
import pytz
import logging
import time as pytime

# Add project root to sys.path to allow importing 'app' modules
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from app.database import SessionLocal, engine, Base
from app.models import StoreStatus, StoreBusinessHours, StoreTimezone
from app.config import settings
from app import models

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- File Paths (Update these if your CSVs are located elsewhere) ---
# Expect data files in a 'data' subdirectory relative to project root
DATA_DIR = os.path.join(project_root, 'data')
STATUS_CSV = os.path.join(DATA_DIR, 'store_status.csv')
HOURS_CSV = os.path.join(DATA_DIR, 'menu_hours.csv')
TIMEZONE_CSV_OPTIONS = [
    os.path.join(DATA_DIR, 'bq-results-20230125-202210-1674678181880.csv'),
    os.path.join(DATA_DIR, 'timezones.csv')
]

# --- MODIFIED find_timezone_csv for Debugging ---
def find_timezone_csv():
    """Finds the timezone CSV file from common names - DEBUG VERSION."""
    logger.info(f"--- Debug: Inside find_timezone_csv ---")
    for i, path in enumerate(TIMEZONE_CSV_OPTIONS):
        logger.info(f"--- Debug: Checking option {i}: '{path}' (Type: {type(path)})")
        try:
            exists = os.path.exists(path)
            logger.info(f"--- Debug: os.path.exists returned: {exists} for option {i}")
            if exists:
                # Check if it's actually a file, not just a directory like C:\
                is_file = os.path.isfile(path)
                logger.info(f"--- Debug: os.path.isfile returned: {is_file} for option {i}")
                if is_file:
                    logger.info(f"--- Debug: Found timezone data file at: '{path}'")
                    return path # Return the valid file path
                else:
                    logger.warning(f"--- Debug: Path exists but is not a file: '{path}'")
            else:
                 logger.info(f"--- Debug: Path does not exist: '{path}'")

        except Exception as e:
            logger.error(f"--- Debug: Error checking path '{path}': {e}")

    logger.warning(f"Could not find a valid timezone CSV file in {DATA_DIR} using names: {[os.path.basename(p) for p in TIMEZONE_CSV_OPTIONS]}")
    return None

def parse_time_robust(time_str, row_info=""):
    """Handles potential missing seconds."""
    if pd.isna(time_str):
        logger.warning(f"Missing time value found {row_info}. Skipping time parse.")
        return None
    try:
        # Try parsing with seconds first
        return datetime.strptime(str(time_str), '%H:%M:%S').time()
    except ValueError:
        try:
           # Fallback to parsing without seconds
           return datetime.strptime(str(time_str), '%H:%M').time()
        except ValueError:
             # Log specific error if both fail
             logger.error(f"Could not parse time: '{time_str}' {row_info}")
             return None

def load_data():
    db: Session = SessionLocal()
    logger.info("Starting data loading process...")

    # --- Ensure data directory and files exist ---
    if not os.path.isdir(DATA_DIR):
        logger.error(f"'data' directory not found at {DATA_DIR}. Please create it and place CSV files inside.")
        db.close()
        return

    status_exists = os.path.exists(STATUS_CSV)
    hours_exists = os.path.exists(HOURS_CSV)
    timezone_csv_path = find_timezone_csv()
    timezone_exists = timezone_csv_path is not None

    if not (status_exists and hours_exists and timezone_exists):
        logger.error("One or more required CSV files not found in the 'data' directory.")
        if not status_exists: logger.error(f"- Missing: {STATUS_CSV}")
        if not hours_exists: logger.error(f"- Missing: {HOURS_CSV}")
        if not timezone_exists: logger.error(f"- Missing timezone CSV (tried: {[os.path.basename(p) for p in TIMEZONE_CSV_OPTIONS]})")
        db.close()
        return

    # Create tables defined in models.py if they don't exist
    try:
        logger.info("Ensuring database tables exist...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables checked/created.")
    except Exception as e:
        logger.error(f"Error creating/checking database tables: {e}", exc_info=True)
        db.close()
        return

    # Optional: Clear existing data for a fresh load (Use with caution!)
    # logger.warning("Clearing existing data from tables...")
    # try:
    #     db.query(models.StoreStatus).delete()
    #     db.query(models.StoreBusinessHours).delete()
    #     db.query(models.StoreTimezone).delete() # Needs careful handling if PK/FKs exist
    #     # Reports table might be kept or cleared depending on requirements
    #     # db.query(models.Report).delete()
    #     db.commit()
    #     logger.info("Existing data cleared.")
    # except Exception as e:
    #     logger.error(f"Error clearing existing data: {e}", exc_info=True)
    #     db.rollback()
    #     db.close()
    #     return


    try:
        # 1. Load Timezones (Upsert logic: Update existing or insert new)
        logger.info(f"Loading timezones from {timezone_csv_path}...")
        tz_df = pd.read_csv(timezone_csv_path)
        tz_df = tz_df.drop_duplicates(subset=['store_id'], keep='last') # Keep latest entry if duplicates exist in CSV
        timezones_to_upsert = []
        loaded_count = 0
        skipped_count = 0

        for _, row in tz_df.iterrows():
            store_id = str(row['store_id']) # Ensure string format
            tz_str = row['timezone_str'] if pd.notna(row['timezone_str']) else 'America/Chicago'

            # Basic validation of timezone string using pytz
            try:
                pytz.timezone(tz_str)
            except pytz.UnknownTimeZoneError:
                 logger.warning(f"Invalid timezone '{tz_str}' for store {store_id}. Defaulting to 'America/Chicago'.")
                 tz_str = 'America/Chicago'
            except Exception: # Catch other potential errors
                logger.warning(f"Error validating timezone '{tz_str}' for store {store_id}. Defaulting to 'America/Chicago'.")
                tz_str = 'America/Chicago'


            existing_tz = db.get(models.StoreTimezone, store_id)
            if existing_tz:
                # Update if different
                if existing_tz.timezone_str != tz_str:
                    existing_tz.timezone_str = tz_str
                    # No need to add to list, session tracks changes
                    loaded_count +=1 # Count as loaded/updated
                else:
                    skipped_count += 1 # No change needed
            else:
                # Insert new
                db.add(models.StoreTimezone(store_id=store_id, timezone_str=tz_str))
                loaded_count += 1

        db.commit() # Commit all updates and inserts
        logger.info(f"Timezones loaded/updated: {loaded_count}. Skipped (no change): {skipped_count}.")


        # 2. Load Business Hours (Delete existing for store, then insert new)
        logger.info(f"Loading business hours from {HOURS_CSV}...")
        hours_df = pd.read_csv(HOURS_CSV)
        # Get unique store IDs present in this CSV
        stores_in_csv = hours_df['store_id'].astype(str).unique()
        business_hours_to_add = []
        hours_processed_count = 0
        hours_skipped_invalid = 0

        # Delete existing hours for stores found in the current CSV batch first
        if stores_in_csv.any(): # Check if the array is not empty
            logger.info(f"Deleting existing business hours for {len(stores_in_csv)} stores found in CSV...")
            delete_q = models.StoreBusinessHours.__table__.delete().where(models.StoreBusinessHours.store_id.in_(stores_in_csv))
            db.execute(delete_q)
            # No commit yet, do it after adding new ones

        processed_store_day = set() # Track (store_id, day) combos within this load batch

        for _, row in hours_df.iterrows():
            row_info = f"(Row Index: {_})" # Add row index for easier debugging
            store_id = str(row['store_id']) # Ensure string
            # Try both 'day' and 'dayOfWeek' column names
            day_val = row.get('day', row.get('dayOfWeek', None))

            if pd.isna(store_id) or pd.isna(day_val):
                logger.warning(f"Skipping business hours row due to missing store_id or day {row_info}: {row.to_dict()}")
                hours_skipped_invalid += 1
                continue

            try:
                day = int(day_val)
                if not (0 <= day <= 6):
                     raise ValueError("Day must be between 0 and 6")
            except ValueError as e:
                logger.warning(f"Skipping business hours row due to invalid day '{day_val}' {row_info}: {e}")
                hours_skipped_invalid += 1
                continue

            # Check for duplicates within this specific CSV load batch
            if (store_id, day) in processed_store_day:
                logger.warning(f"Duplicate day entry in CSV for store {store_id}, day {day} {row_info}. Keeping first encountered.")
                # Skip subsequent duplicates in the file
                continue

            start_time = parse_time_robust(row['start_time_local'], row_info + f" store={store_id}")
            end_time = parse_time_robust(row['end_time_local'], row_info + f" store={store_id}")

            if start_time is None or end_time is None:
                logger.error(f"Failed to parse time for store {store_id}, day {day} {row_info}. Skipping row.")
                hours_skipped_invalid += 1
                continue

            business_hours_to_add.append(models.StoreBusinessHours(
                store_id=store_id,
                day_of_week=day,
                start_time_local=start_time,
                end_time_local=end_time
            ))
            processed_store_day.add((store_id, day)) # Mark as processed for this run
            hours_processed_count += 1


        if business_hours_to_add:
            db.bulk_save_objects(business_hours_to_add)
            # Commit after deletes and inserts for this batch
            db.commit()
            logger.info(f"Loaded {hours_processed_count} new business hours entries.")
        else:
             logger.info("No valid new business hours entries to load from CSV.")
        if hours_skipped_invalid > 0:
            logger.warning(f"Skipped {hours_skipped_invalid} invalid business hours rows.")


        # 3. Load Status Data (Append only - assuming polls are immutable history)
        logger.info(f"Loading status data from {STATUS_CSV}...")
        # Use chunking for large files
        chunk_size = 50000 # Adjust based on memory constraints
        total_status_count = 0
        status_added_count = 0
        status_skipped_count = 0 # Could add check for existing timestamp/store combo if needed

        for chunk_df in pd.read_csv(STATUS_CSV, chunksize=chunk_size, low_memory=False):
            store_statuses_to_add = []
            chunk_start_time = pytime.time()

            # Pre-process timestamp column efficiently
            chunk_df['timestamp_utc_str'] = chunk_df['timestamp_utc'].astype(str) # Work with string copy
            chunk_df['timestamp_utc_str'] = chunk_df['timestamp_utc_str'].str.replace(' UTC', '', regex=False)
            # Parse to datetime objects, coercing errors to NaT
            chunk_df['timestamp_dt'] = pd.to_datetime(chunk_df['timestamp_utc_str'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')

            # Filter out rows where timestamp parsing failed
            invalid_timestamps = chunk_df['timestamp_dt'].isna()
            if invalid_timestamps.any():
                logger.warning(f"Found {invalid_timestamps.sum()} rows with invalid timestamps in chunk. Skipping them.")
                # Optional: Log the failing rows here if needed for debugging
                chunk_df = chunk_df.dropna(subset=['timestamp_dt'])

            if chunk_df.empty:
                logger.info("Skipping empty or fully invalid chunk.")
                continue

            # Prepare data for bulk insert
            for _, row in chunk_df.iterrows():
                 # Basic validation for status
                status = str(row['status']).lower()
                if status not in ['active', 'inactive']:
                     logger.warning(f"Invalid status '{row['status']}' for store {row['store_id']} at {row['timestamp_dt']}. Skipping row.")
                     status_skipped_count += 1
                     continue

                # Assuming we append all valid rows. Add check for existence if needed.
                store_statuses_to_add.append(models.StoreStatus(
                    store_id=str(row['store_id']), # Ensure string
                    # Convert pandas Timestamp to python datetime and make tz-aware
                    timestamp_utc=pytz.utc.localize(row['timestamp_dt'].to_pydatetime()),
                    status=status
                ))

            if store_statuses_to_add:
                # Use bulk_insert_mappings for potential speedup, requires dicts
                # status_mappings = [s.__dict__ for s in store_statuses_to_add]
                # db.bulk_insert_mappings(models.StoreStatus, status_mappings)
                # OR stick with bulk_save_objects
                db.bulk_save_objects(store_statuses_to_add)
                # Commit per chunk to manage memory and transaction size
                db.commit()
                status_added_count += len(store_statuses_to_add)
                chunk_end_time = pytime.time()
                logger.info(f"Processed chunk: Added {len(store_statuses_to_add)} status entries in {chunk_end_time - chunk_start_time:.2f}s. Total added: {status_added_count}")
            total_status_count += len(chunk_df) # Count rows processed in chunk

        logger.info(f"Finished loading status data. Total rows processed: {total_status_count}, Added: {status_added_count}, Skipped: {status_skipped_count}")

        logger.info("Data loading process finished successfully.")

    except Exception as e:
        db.rollback() # Rollback any partial changes from the failed transaction
        logger.error(f"An error occurred during data loading: {e}", exc_info=True)
    finally:
        db.close() # Always close the session

if __name__ == "__main__":
    load_data()