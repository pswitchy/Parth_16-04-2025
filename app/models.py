# app/models.py
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Time, Enum as SQLAlchemyEnum, ForeignKey
from sqlalchemy.orm import relationship
from .database import Base
import enum
import uuid
from sqlalchemy.sql import func # For server_default timestamp

# Define UUID length (standard 36 characters)
UUID_LENGTH = 36

class StoreStatus(Base):
    __tablename__ = "store_status"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String(UUID_LENGTH), index=True) # Changed to String
    timestamp_utc = Column(DateTime(timezone=True), index=True)
    status = Column(String(10), index=True) # "active" or "inactive"

class StoreBusinessHours(Base):
    __tablename__ = "store_business_hours"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String(UUID_LENGTH), index=True) # Changed to String
    day_of_week = Column(Integer) # 0=Monday, 6=Sunday
    start_time_local = Column(Time)
    end_time_local = Column(Time)

class StoreTimezone(Base):
    __tablename__ = "store_timezone"
    # Removed id, using store_id as primary key for simplicity and uniqueness guarantee
    store_id = Column(String(UUID_LENGTH), primary_key=True, index=True) # Changed to String, made PK
    timezone_str = Column(String, default="America/Chicago")

class ReportStatusEnum(str, enum.Enum):
    RUNNING = "Running"
    COMPLETE = "Complete"
    FAILED = "Failed"

class Report(Base):
    __tablename__ = "reports"
    report_id = Column(String(UUID_LENGTH), primary_key=True, index=True, default=lambda: str(uuid.uuid4())) # Made PK
    status = Column(SQLAlchemyEnum(ReportStatusEnum), default=ReportStatusEnum.RUNNING)
    file_path = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now()) # Use func.now() for portability
    updated_at = Column(DateTime(timezone=True), onupdate=func.now()) # Track updates


# Optional: Define relationships if needed for complex queries,
# though not strictly required for this specific logic.
# Ensure ForeignKey constraints match the new String type if relationships are used.