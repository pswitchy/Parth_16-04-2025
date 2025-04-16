# app/report_logic.py
import pytz
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, time, date
from typing import List, Dict, Tuple, Optional
from . import crud, models
import logging
import functools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Type hint for store_id is now str
STORE_ID_TYPE = str

# --- Caching Timezone Objects ---
@functools.lru_cache(maxsize=256)
def get_pytz_timezone(timezone_str: str) -> pytz.BaseTzInfo:
    """Gets (and caches) pytz timezone object from string."""
    # Default to UTC if timezone_str is None or empty for safety
    if not timezone_str:
        logger.warning("Received empty timezone string. Defaulting to UTC.")
        return pytz.utc
    try:
        return pytz.timezone(timezone_str)
    except pytz.UnknownTimeZoneError:
        logger.warning(f"Unknown timezone '{timezone_str}'. Defaulting to America/Chicago.")
        # Fallback to a known default timezone
        return pytz.timezone("America/Chicago")
    except Exception as e:
        # Catch any other unexpected errors during timezone loading
        logger.error(f"Error loading timezone '{timezone_str}': {e}. Defaulting to America/Chicago.")
        return pytz.timezone("America/Chicago")

# --- Business Hour Calculation (No change in logic from prev full version) ---
def get_utc_business_intervals(
    target_date_utc: date,
    business_hours: List[models.StoreBusinessHours], # List of ORM objects
    store_timezone: pytz.BaseTzInfo # Accepts the pytz object directly
) -> List[Tuple[datetime, datetime]]:
    """
    Calculates the UTC intervals corresponding to business hours for a specific UTC date.
    Handles overnight shifts and DST transitions implicitly via pytz.
    """
    intervals_utc = []

    # Consider the target UTC day and the day before/after to catch overnight shifts crossing UTC midnight
    for day_offset in [-1, 0, 1]:
        current_local_date = target_date_utc + timedelta(days=day_offset)
        day_of_week = current_local_date.weekday() # Monday is 0

        for hours in business_hours:
            # Check if store_id matches if StoreBusinessHours object contains it
            # (it does in our model) - this check might be redundant if list is pre-filtered
            # but added for clarity/safety if rules were mixed
            # if hours.store_id != store_id_being_processed: continue

            if hours.day_of_week == day_of_week:
                # Combine the date part with the time part
                start_local_naive = datetime.combine(current_local_date, hours.start_time_local)
                end_local_naive = datetime.combine(current_local_date, hours.end_time_local)

                # Handle overnight shifts (where end time is on the *next* local day)
                if hours.end_time_local <= hours.start_time_local:
                     end_local_naive += timedelta(days=1)

                try:
                    # Localize using the store's timezone - this handles DST
                    start_local_aware = store_timezone.localize(start_local_naive, is_dst=None)
                    end_local_aware = store_timezone.localize(end_local_naive, is_dst=None)
                except (pytz.NonExistentTimeError, pytz.AmbiguousTimeError) as e:
                    # Log DST issue more informatively
                    logger.warning(f"DST issue for store {hours.store_id} on {current_local_date} at {start_local_naive.time()}/{end_local_naive.time()}: {e}. Trying DST fallback.")
                    # Attempt fallback (e.g., force standard time) - adjust as needed
                    try:
                        # Try forcing standard time (is_dst=False)
                        start_local_aware = store_timezone.localize(start_local_naive, is_dst=False)
                        end_local_aware = store_timezone.localize(end_local_naive, is_dst=False)
                    except Exception as fallback_e:
                        logger.error(f"DST fallback failed for store {hours.store_id} on {current_local_date}: {fallback_e}. Skipping this specific interval part.")
                        continue # Skip this specific interval part on error
                except Exception as e:
                     # Catch other potential localization errors
                     logger.error(f"Unexpected error localizing time for store {hours.store_id} on {current_local_date}: {e}", exc_info=True)
                     continue

                # Convert to UTC
                start_utc = start_local_aware.astimezone(pytz.utc)
                end_utc = end_local_aware.astimezone(pytz.utc)

                # Clip the interval to the bounds of the *target UTC date*
                target_day_start_utc = datetime.combine(target_date_utc, time(0, 0), tzinfo=pytz.utc)
                target_day_end_utc = target_day_start_utc + timedelta(days=1)

                # Calculate the actual overlapping interval with the target UTC day
                overlap_start = max(start_utc, target_day_start_utc)
                overlap_end = min(end_utc, target_day_end_utc)

                # Only add if there's a valid overlap duration
                if overlap_end > overlap_start:
                    intervals_utc.append((overlap_start, overlap_end))

    # --- Sort and merge overlapping/adjacent intervals ---
    if not intervals_utc:
        return []

    intervals_utc.sort() # Sort by start time

    merged_intervals = []
    if intervals_utc:
        # Initialize with the first interval
        current_start, current_end = intervals_utc[0]

        for next_start, next_end in intervals_utc[1:]:
            if next_start < current_end: # Check for overlap only (<= allows adjacent merging)
                # Merge overlapping intervals by extending the current end time if necessary
                current_end = max(current_end, next_end)
            else:
                # No overlap, finalize the previous interval and start a new one
                merged_intervals.append((current_start, current_end))
                current_start, current_end = next_start, next_end

        # Add the last processed interval
        merged_intervals.append((current_start, current_end))

    return merged_intervals


# --- Main Calculation Function ---
def calculate_store_uptime_downtime(
    store_id: STORE_ID_TYPE,
    current_timestamp_utc: datetime,
    db: Session # Requires a DB session for this specific store
) -> Optional[Dict[str, float]]: # Return Optional to indicate potential errors
    """
    Calculates uptime and downtime for a single store for the last hour, day, and week.
    Takes an active database session as input.
    Returns None if a critical error occurs during calculation for this store.
    """
    try:
        # Define reporting intervals relative to current_timestamp_utc
        one_hour_ago = current_timestamp_utc - timedelta(hours=1)
        one_day_ago = current_timestamp_utc - timedelta(days=1)
        one_week_ago = current_timestamp_utc - timedelta(weeks=1)

        # Get store metadata using the provided session
        timezone_str = crud.get_store_timezone(db, store_id)
        store_timezone = get_pytz_timezone(timezone_str) # Use cached getter

        business_hours_rules = crud.get_store_business_hours(db, store_id) # Handles 24/7 default

        # Get status polls for the relevant period (last week + one poll before)
        # Fetch slightly earlier to ensure the interval starting the week is correct
        poll_fetch_start = one_week_ago - timedelta(hours=2) # Buffer for interpolation start
        status_polls = crud.get_store_status_in_window(db, store_id, poll_fetch_start, current_timestamp_utc)

        # Initialize uptime/downtime totals (in seconds initially)
        uptime_last_hour_sec = 0.0
        downtime_last_hour_sec = 0.0
        uptime_last_day_sec = 0.0
        downtime_last_day_sec = 0.0
        uptime_last_week_sec = 0.0
        downtime_last_week_sec = 0.0

        if not status_polls:
            # If no polls exist *at all* in the fetch window (week + buffer), we need to decide the baseline.
            # Option 1: Assume unknown/0 uptime/downtime.
            # Option 2: If business_hours_rules imply it *should* be open, maybe count as downtime?
            # Let's stick to Option 1 based on the "extrapolate from observations" requirement.
            logger.info(f"No status polls found for store {store_id} in the relevant time window ({poll_fetch_start} to {current_timestamp_utc}). Reporting 0s.")
            # Return 0s directly
            return {
                "store_id": store_id,
                "uptime_last_hour": 0, "uptime_last_day": 0, "uptime_last_week": 0,
                "downtime_last_hour": 0, "downtime_last_day": 0, "downtime_last_week": 0
            }

        # --- Interpolate status between polls ---
        for i in range(len(status_polls)):
            current_poll = status_polls[i]
            status = current_poll.status.lower() # Ensure lowercase 'active'/'inactive'

            # Define the start time of the interval this poll's status covers
            interval_start = current_poll.timestamp_utc

            # Define the end time of the interval
            if i + 1 < len(status_polls):
                next_poll = status_polls[i+1]
                interval_end = next_poll.timestamp_utc
            else:
                # Last poll's status extends to the current reporting time
                interval_end = current_timestamp_utc

            # Clip interval start to the beginning of our analysis window (one week ago)
            # Important for the *first* interval being processed.
            interval_start_clipped = max(interval_start, one_week_ago)

            # Clip interval end to the reporting time
            interval_end_clipped = min(interval_end, current_timestamp_utc)


            # Skip if the resulting interval is invalid or zero-duration
            if interval_end_clipped <= interval_start_clipped:
                continue

            # --- Iterate through each UTC day the interval touches ---
            start_date_utc = interval_start_clipped.astimezone(pytz.utc).date()
            end_date_utc = interval_end_clipped.astimezone(pytz.utc).date()

            # Loop through dates from start_date_utc up to and including end_date_utc
            current_processing_date = start_date_utc
            while current_processing_date <= end_date_utc:
                # Get business hours UTC intervals for this specific date
                # Pass the actual store_timezone object obtained earlier
                business_intervals_utc = get_utc_business_intervals(
                    current_processing_date,
                    business_hours_rules,
                    store_timezone
                    )

                # Calculate intersections for this day
                for bh_start, bh_end in business_intervals_utc:
                    # Find intersection of the *current status interval* and the *business hours interval for this day*
                    overlap_start = max(interval_start_clipped, bh_start)
                    overlap_end = min(interval_end_clipped, bh_end)

                    # If an overlap exists within this day's business hours
                    if overlap_end > overlap_start:
                        # Calculate duration within the specific reporting windows
                        # Last Hour Check
                        lh_report_start = max(overlap_start, one_hour_ago)
                        lh_report_end = min(overlap_end, current_timestamp_utc)
                        if lh_report_end > lh_report_start:
                            lh_duration = (lh_report_end - lh_report_start).total_seconds()
                            if status == 'active':
                                uptime_last_hour_sec += lh_duration
                            else:
                                downtime_last_hour_sec += lh_duration

                        # Last Day Check
                        ld_report_start = max(overlap_start, one_day_ago)
                        ld_report_end = min(overlap_end, current_timestamp_utc)
                        if ld_report_end > ld_report_start:
                            ld_duration = (ld_report_end - ld_report_start).total_seconds()
                            if status == 'active':
                                uptime_last_day_sec += ld_duration
                            else:
                                downtime_last_day_sec += ld_duration

                        # Last Week Check (overlap is already clipped to >= one_week_ago by interval_start_clipped)
                        lw_report_start = overlap_start # Equivalent to max(overlap_start, one_week_ago)
                        lw_report_end = overlap_end   # Equivalent to min(overlap_end, current_timestamp_utc)
                        # Re-check needed if overlap_end could exceed current_timestamp_utc (shouldn't happen here)
                        if lw_report_end > lw_report_start:
                             lw_duration = (lw_report_end - lw_report_start).total_seconds()
                             if status == 'active':
                                 uptime_last_week_sec += lw_duration
                             else:
                                downtime_last_week_sec += lw_duration

                # Move to the next date
                current_processing_date += timedelta(days=1)
        # --- End of poll interpolation loop ---

        # --- Final conversion and result formatting ---
        report_data = {
            "store_id": store_id,
            # Convert seconds to minutes (for hour) or hours (for day/week) and round
            "uptime_last_hour": int(round(uptime_last_hour_sec / 60)), # Round to nearest minute
            "uptime_last_day": round(uptime_last_day_sec / 3600, 2),
            "uptime_last_week": round(uptime_last_week_sec / 3600, 2),
            "downtime_last_hour": int(round(downtime_last_hour_sec / 60)), # Round to nearest minute
            "downtime_last_day": round(downtime_last_day_sec / 3600, 2),
            "downtime_last_week": round(downtime_last_week_sec / 3600, 2),
        }
        return report_data

    except Exception as e:
        # Log the error with store ID for better debugging
        logger.error(f"Error calculating report for store {store_id}: {e}", exc_info=True)
        return None # Indicate failure for this specific store