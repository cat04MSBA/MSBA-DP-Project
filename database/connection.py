import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def get_engine():
    """
    Returns a SQLAlchemy engine connected to Supabase.
    Reads DATABASE_URL from .env file.
    Import this in every ingestion and transformation script.
    
    Usage:
        from database.connection import get_engine
        engine = get_engine()
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise ValueError(
            "DATABASE_URL not set. "
            "Copy .env.example to .env and fill in your credentials."
        )
    return create_engine(url)