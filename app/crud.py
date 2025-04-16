# app/crud.py
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct, select
from . import models
import datetime
from typing import List, Optional, Tuple

# Type hint for store_id is now str
STORE_ID_TYPE = str

def get_store_timezone(db: Session, store_id: STORE_ID_TYPE) -> str:
    # Querying directly on the primary key now
    tz = db.query(models.StoreTimezone.timezone_str).filter(models.StoreTimezone.store_id == store_id).first()
    return tz[0] if tz else "America/Chicago"

def get_store_business_hours(db: Session, store_id: STORE_ID_TYPE) -> List[models.StoreBusinessHours]:
    hours = db.query(models.StoreBusinessHours).filter(models.StoreBusinessHours.store_id == store_id).all()
    # If no hours found, assume 24/7
    if not hours:
        # Generate default 24/7 rules if none exist for the store
        return [
            models.StoreBusinessHours(
                store_id=store_id,
                day_of_week=i,
                start_time_local=datetime.time(0, 0),
                end_time_local=datetime.time(23, 59, 59, 999999) # End of day
            ) for i in range(7)
        ]
    return hours

def get_store_status_in_window(db: Session, store_id: STORE_ID_TYPE, start_time: datetime.datetime, end_time: datetime.datetime) -> List[models.StoreStatus]:
    """Fetches status polls within the window, plus the last one before the window."""

    # Last poll strictly BEFORE the window start
    last_poll_before_query = select(models.StoreStatus)\
        .where(models.StoreStatus.store_id == store_id, models.StoreStatus.timestamp_utc < start_time)\
        .order_by(models.StoreStatus.timestamp_utc.desc())\
        .limit(1)
    last_poll_before = db.execute(last_poll_before_query).scalar_one_or_none()


    # Polls WITHIN the window (inclusive of end_time)
    polls_within_query = select(models.StoreStatus)\
        .where(
            models.StoreStatus.store_id == store_id,
            models.StoreStatus.timestamp_utc >= start_time,
            models.StoreStatus.timestamp_utc <= end_time
        )\
        .order_by(models.StoreStatus.timestamp_utc.asc())
    polls_within = db.execute(polls_within_query).scalars().all()


    results = []
    if last_poll_before:
        results.append(last_poll_before)
    results.extend(polls_within)

    # Deduplicate based on timestamp (rare case) and ensure final sort
    unique_results = {p.timestamp_utc: p for p in results}
    return sorted(unique_results.values(), key=lambda p: p.timestamp_utc)


def get_max_timestamp(db: Session) -> Optional[datetime.datetime]:
    # Using func.max correctly
    max_ts = db.query(func.max(models.StoreStatus.timestamp_utc)).scalar()
    return max_ts

def get_all_store_ids(db: Session) -> List[STORE_ID_TYPE]:
    # Get unique store IDs from the status table
    # Using select() for consistency
    query = select(distinct(models.StoreStatus.store_id))
    result = db.execute(query).scalars().all()
    return result # Already returns a list of store_ids (strings)

def create_report(db: Session, report_id: str) -> models.Report:
    # Creates a new report entry with RUNNING status
    db_report = models.Report(report_id=report_id, status=models.ReportStatusEnum.RUNNING)
    db.add(db_report)
    db.commit()
    db.refresh(db_report)
    return db_report

def get_report(db: Session, report_id: str) -> Optional[models.Report]:
    # Querying directly on the primary key now
    return db.get(models.Report, report_id) # Use db.get for PK lookup

def update_report_status(db: Session, report_id: str, status: models.ReportStatusEnum, file_path: Optional[str] = None):
    # Fetches report by PK and updates status/filepath
    db_report = db.get(models.Report, report_id)
    if db_report:
        db_report.status = status
        db_report.file_path = file_path
        # updated_at will be handled by DB 'onupdate' trigger if configured,
        # or manually set it: db_report.updated_at = func.now() / datetime.now(pytz.utc)
        db.commit()
    else:
        # Log or handle the case where the report to update wasn't found
        print(f"Warning: Report with ID {report_id} not found for status update.")