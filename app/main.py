# app/main.py
import os
import uuid
import pandas as pd
from fastapi import FastAPI, Depends, BackgroundTasks, HTTPException, Path
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
from . import crud, models, report_logic
from .database import engine as main_engine, get_db, SessionLocal as MainSessionLocal
from .config import settings
from datetime import datetime
import pytz
import logging
import multiprocessing
import time as pytime
from typing import Optional, Dict, List, Tuple # For type hints

# Ensure the report directory exists
os.makedirs(settings.report_dir, exist_ok=True)

# Create tables if they don't exist using the main engine
try:
    models.Base.metadata.create_all(bind=main_engine)
except Exception as e:
    print(f"Error creating database tables: {e}") # Added basic error logging for table creation

app = FastAPI(title="Store Monitoring API")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Configure multiprocessing logger to avoid duplicate handlers if run multiple times
# Get the multiprocessing logger - use getLogger directly
mpl_logger = logging.getLogger('multiprocessing')
if not mpl_logger.hasHandlers(): # Add handler only if none exists
    mpl_logger.propagate = False # Prevent propagation to root logger if handlers are added
    mpl_handler = logging.StreamHandler()
    mpl_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    mpl_handler.setFormatter(mpl_formatter)
    mpl_logger.addHandler(mpl_handler)
mpl_logger.setLevel(logging.INFO) # Set desired level

logger = logging.getLogger(__name__)


# --- Type Hint Definitions ---
STORE_ID_TYPE = str # Matching models/crud
SingleStoreResult = Optional[Dict[str, float]] # Result from calculation function
MultiProcArgs = Tuple[STORE_ID_TYPE, datetime, str] # Args tuple for worker

# --- Multiprocessing Worker Function ---
def _calculate_single_store_report_wrapper(args: MultiProcArgs) -> SingleStoreResult:
    """
    Wrapper function executed by each process in the pool.
    Creates its own database engine and session.
    """
    store_id, current_time_utc, db_url = args
    # Create a new engine and session specific to this process
    # Use pool settings appropriate for multiprocessing if needed (e.g., NullPool)
    worker_engine = create_engine(db_url,
                                  poolclass=None, # Often recommended for multiprocessing children
                                  connect_args={"check_same_thread": False} if "sqlite" in db_url else {})
    WorkerSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=worker_engine)
    db: Session = WorkerSessionLocal()
    result: SingleStoreResult = None
    try:
        # Call the core logic function
        result = report_logic.calculate_store_uptime_downtime(store_id, current_time_utc, db)
        # Optional: Add logging within the worker process
        # if result:
        #     logger.info(f"[Worker Process] Successfully processed store {store_id}")
        # else:
        #     logger.warning(f"[Worker Process] Calculation returned None for store {store_id}")

    except Exception as e:
        # Log error specific to this store/process
        # Use the standard logger, as multiprocessing logger might not be configured the same way here
        logging.error(f"[Worker Process {os.getpid()}] Error processing store {store_id}: {e}", exc_info=True)
        result = None # Ensure failure is indicated by None
    finally:
        # Ensure database resources are closed properly
        if db:
            db.close()
        if worker_engine:
            worker_engine.dispose() # Dispose engine connections for this process
    return result # Return the dictionary or None

# --- Background Task for Report Generation (Using Multiprocessing) ---
def generate_report_task(report_id: str):
    """The actual function run by BackgroundTasks, orchestrates multiprocessing"""
    start_time = pytime.time()
    # Use the main SessionLocal for initial setup and final DB update
    db_main: Session = MainSessionLocal()
    generated_file_path: Optional[str] = None # Store path for potential cleanup on failure

    try:
        logger.info(f"Starting report generation for report_id: {report_id}")

        # Get current time (max timestamp) using the main session
        current_time = crud.get_max_timestamp(db_main)
        if not current_time:
            logger.error(f"Report {report_id}: No status data found in the database. Cannot generate report.")
            crud.update_report_status(db_main, report_id, models.ReportStatusEnum.FAILED)
            db_main.close() # Close session before returning
            return

        # Ensure current_time is timezone-aware UTC
        if current_time.tzinfo is None:
            current_time = pytz.utc.localize(current_time)
        else:
            current_time = current_time.astimezone(pytz.utc)
        logger.info(f"Report {report_id}: Using current time (max timestamp): {current_time}")

        # Get all store IDs using the main session
        store_ids: List[STORE_ID_TYPE] = crud.get_all_store_ids(db_main)
        if not store_ids:
            logger.warning(f"Report {report_id}: No stores found with status data to generate report for.")
            report_data: List[Dict] = [] # Initialize as empty list
        else:
            # Prepare arguments for worker processes
            task_args: List[MultiProcArgs] = [(store_id, current_time, settings.database_url) for store_id in store_ids]
            num_stores = len(store_ids)
             # Determine number of processes: Use CPU count, but not more than stores, and maybe cap it (e.g., 8) to avoid overwhelming DB
            cpu_cores = os.cpu_count() or 1 # Default to 1 if cpu_count returns None
            # num_processes = min(cpu_cores, num_stores, 8) # Example cap at 8
            num_processes = min(cpu_cores, num_stores)
            logger.info(f"Report {report_id}: Processing {num_stores} stores using {num_processes} worker processes.")

            pool_results: List[SingleStoreResult] = []
            try:
                 # Use multiprocessing Pool with a context manager
                with multiprocessing.Pool(processes=num_processes) as pool:
                    # Use map for simpler arg passing if wrapper takes only one arg, or starmap for tuples
                    pool_results = pool.map(_calculate_single_store_report_wrapper, task_args)
            except Exception as pool_exc:
                 logger.error(f"Report {report_id}: Multiprocessing pool failed: {pool_exc}", exc_info=True)
                 crud.update_report_status(db_main, report_id, models.ReportStatusEnum.FAILED)
                 db_main.close()
                 return # Exit if the pool itself fails

            # Filter out None results (errors from individual stores)
            report_data = [res for res in pool_results if res is not None]
            num_failed = num_stores - len(report_data)
            if num_failed > 0:
                 logger.warning(f"Report {report_id}: Failed to calculate report for {num_failed} out of {num_stores} stores.")
            if not report_data and num_stores > 0:
                 logger.error(f"Report {report_id}: All store calculations failed.")
                 crud.update_report_status(db_main, report_id, models.ReportStatusEnum.FAILED)
                 db_main.close()
                 return # Exit if all stores failed


        # --- Create CSV ---
        report_df = pd.DataFrame(report_data)
        # Define columns in the desired order
        columns_order = [
            "store_id", "uptime_last_hour", "uptime_last_day", "uptime_last_week",
            "downtime_last_hour", "downtime_last_day", "downtime_last_week"
        ]
        if report_df.empty:
            # Create empty DataFrame with correct columns if no data/all failed
            logger.warning(f"Report {report_id}: Generated report is empty.")
            report_df = pd.DataFrame(columns=columns_order)
        else:
             # Reindex DataFrame to ensure all columns are present and in order, fill missing stores/values with 0
            report_df = report_df.reindex(columns=columns_order, fill_value=0)
             # Ensure store_id column is first if it wasn't included in `columns_order` initially or got reordered
            if "store_id" in report_df.columns and columns_order[0] == "store_id":
                report_df = report_df[["store_id"] + [col for col in columns_order if col != "store_id"]]
            else:
                # Fallback if store_id is missing or order is unexpected
                report_df = report_df.reindex(columns=columns_order, fill_value=0)

        generated_file_path = os.path.join(settings.report_dir, f"{report_id}.csv")
        try:
            report_df.to_csv(generated_file_path, index=False)
            logger.info(f"Report {report_id}: CSV generated successfully at {generated_file_path}")
        except Exception as csv_exc:
             logger.error(f"Report {report_id}: Failed to write CSV file {generated_file_path}: {csv_exc}", exc_info=True)
             crud.update_report_status(db_main, report_id, models.ReportStatusEnum.FAILED)
             db_main.close()
             # Attempt to clean up partially written file? Might be risky.
             return

        # Update report status in DB using the main session
        crud.update_report_status(db_main, report_id, models.ReportStatusEnum.COMPLETE, file_path=generated_file_path)
        end_time = pytime.time()
        logger.info(f"Report {report_id}: Status updated to COMPLETE. Total generation time: {end_time - start_time:.2f} seconds.")

    except Exception as e:
        # Catch-all for errors in the main task logic (outside the pool/CSV writing)
        logger.error(f"Report {report_id}: Unexpected error during report generation: {e}", exc_info=True)
        crud.update_report_status(db_main, report_id, models.ReportStatusEnum.FAILED)
        # Clean up generated file if it exists and failure happened after creation
        if generated_file_path and os.path.exists(generated_file_path):
            try:
                os.remove(generated_file_path)
                logger.info(f"Report {report_id}: Cleaned up failed report file {generated_file_path}")
            except OSError as rm_err:
                logger.error(f"Report {report_id}: Failed to clean up report file {generated_file_path}: {rm_err}")
    finally:
        # Always ensure the main database session is closed
        if db_main:
            db_main.close()

# --- API Endpoints (Logic unchanged, added response models for clarity) ---
# Optional: Define Pydantic models for request/response validation
from pydantic import BaseModel

class TriggerResponse(BaseModel):
    report_id: str

class ReportStatusResponse(BaseModel):
    status: str

@app.post("/trigger_report",
          response_model=TriggerResponse,
          summary="Trigger Background Report Generation")
async def trigger_report(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Triggers the generation of the store uptime/downtime report.

    The report calculation runs in the background using multiprocessing.
    Returns a unique `report_id` which can be used to poll the status
    and retrieve the report via the `/get_report/{report_id}` endpoint.
    """
    report_id = str(uuid.uuid4())
    try:
        crud.create_report(db, report_id)
        # Add the task to FastAPI's background tasks
        background_tasks.add_task(generate_report_task, report_id)
        logger.info(f"Report generation triggered with report_id: {report_id}")
        return {"report_id": report_id}
    except Exception as e:
        logger.error(f"Failed to trigger report generation for potential ID {report_id}: {e}", exc_info=True)
        # Consider rolling back the DB entry if triggering failed fundamentally
        # db.rollback()
        raise HTTPException(status_code=500, detail="Internal server error triggering report generation.")

@app.get("/get_report/{report_id}",
         summary="Get Report Status or CSV File",
         responses={
             200: {
                 "content": {
                     "application/json": {
                         "schema": {"$ref": "#/components/schemas/ReportStatusResponse"}
                     },
                     "text/csv": {
                         "schema": {"type": "string", "format": "binary"}
                     }
                 },
                 "description": "Returns 'Running' or 'Failed' status as JSON, or the completed report as CSV.",
             },
             404: {"description": "Report not found"},
             500: {"description": "Report marked complete but file is missing"},
         })
async def get_report(
    report_id: str = Path(..., description="The ID of the report previously triggered."),
    db: Session = Depends(get_db)
):
    """
    Retrieves the status of a previously triggered report generation task.

    - If the report generation is still **Running**, returns `{"status": "Running"}`.
    - If the report generation **Failed**, returns `{"status": "Failed"}`.
    - If the report generation is **Complete**, returns the generated report
      as a CSV file download.
    - Returns **404 Not Found** if the `report_id` does not exist.
    - Returns **500 Internal Server Error** if the report status is Complete,
      but the associated CSV file cannot be found on the server.
    """
    logger.debug(f"Received GET request for report_id: {report_id}")
    # Use db.get for efficient primary key lookup
    db_report = db.get(models.Report, report_id)

    if db_report is None:
        logger.warning(f"Report GET request: Report not found for report_id: {report_id}")
        raise HTTPException(status_code=404, detail=f"Report with ID '{report_id}' not found.")

    report_status = db_report.status

    if report_status == models.ReportStatusEnum.COMPLETE:
        file_path = db_report.file_path
        if file_path and os.path.exists(file_path):
            logger.info(f"Report {report_id} is COMPLETE. Returning file: {file_path}")
            # Return the CSV file
            return FileResponse(
                path=file_path,
                media_type='text/csv',
                filename=f"report_{report_id}.csv" # Suggest a filename for download
            )
        else:
            logger.error(f"Report {report_id} status is COMPLETE, but file is missing or path is null. Path: {file_path}")
            # Optional: Update status back to FAILED if file is missing?
            # crud.update_report_status(db, report_id, models.ReportStatusEnum.FAILED)
            # db.commit()
            raise HTTPException(status_code=500, detail="Report processing complete, but the report file is missing.")

    elif report_status == models.ReportStatusEnum.FAILED:
        logger.warning(f"Report {report_id} status is FAILED.")
        # Return status Failed as JSON
        return JSONResponse(content={"status": report_status.value}, status_code=200)

    else: # Status must be RUNNING
        logger.info(f"Report {report_id} status is RUNNING.")
        # Return status Running as JSON
        return JSONResponse(content={"status": report_status.value}, status_code=200)