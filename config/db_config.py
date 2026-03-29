"""
Database configuration — reads connection settings from .env file.
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env
load_dotenv()

DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER", "pipeline_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "ecommerce"),
}


def get_connection_url():
    """Build PostgreSQL connection URL for SQLAlchemy."""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


def get_engine():
    """Create and return a SQLAlchemy engine."""
    return create_engine(get_connection_url())
