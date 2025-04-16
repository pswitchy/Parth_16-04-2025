# app/database.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from .config import settings
import logging

logger = logging.getLogger(__name__)

try:
    # Define connection args, especially for SQLite to allow multi-threaded access from BackgroundTasks
    connect_args = {"check_same_thread": False} if "sqlite" in settings.database_url else {}

    engine = create_engine(
        settings.database_url,
        connect_args=connect_args,
        pool_pre_ping=True # Good practice for ensuring connections are alive
    )

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    Base = declarative_base()

    logger.info("Database engine and session configured successfully.")

except Exception as e:
    logger.error(f"Failed to configure database engine: {e}", exc_info=True)
    # Raise or exit? Depending on desired behavior if DB setup fails.
    raise

# Dependency to get DB session for FastAPI endpoints
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()