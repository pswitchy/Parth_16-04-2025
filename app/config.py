# app/config.py
import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Loading .env file at the project root
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=dotenv_path)

class Settings(BaseSettings):
    database_url: str = os.getenv("DATABASE_URL")
    report_dir: str = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'reports')

settings = Settings()

# This line will check if the report directory exists
os.makedirs(settings.report_dir, exist_ok=True)