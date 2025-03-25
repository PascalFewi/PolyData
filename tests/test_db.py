import os
from dotenv import load_dotenv
from src.database import get_db_connection, insert_metadata
from src.models import DatabaseConfig, MetadataEntry

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
from datetime import datetime, timezone

def test_entry(database_config: DatabaseConfig):
    metadata = MetadataEntry(
        market_id="515539",
        hour=12,
        date="2024-12-17",  # ISO string format
        fetched_at=datetime.now(timezone.utc),
        slug="will-bitcoin-hit",
        condition_id="condition-123",
        clob_token_id="clob-123",
        start_time="2024-12-17T00:00:00",
        end_time="2024-12-17T01:00:00",
        num_updates=10,
        order_price_min_tick_size=0.01,
        order_min_size=1.0,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )

    insert_metadata(metadata, "/path/to/file.json", database_config)

def test_connection(database_config: DatabaseConfig):
    """
    Test the database connection.
    Args:
        database_config (DatabaseConfig): The database configuration.
    """
    conn = get_db_connection(database_config)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")  # Simple query to test connectivity
            result = cur.fetchone()
            print(f"Database connection test result: {result}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Initialize the DatabaseConfig object
    database_config = DatabaseConfig(
        DB_HOST=os.getenv("DB_HOST"),
        DB_PORT=os.getenv("DB_PORT"),
        DB_NAME=os.getenv("DB_NAME"),
        DB_USER=os.getenv("DB_USER"),
        DB_PASSWORD=os.getenv("DB_PASSWORD"),
    )

    # Test the connection
    test_connection(database_config)
    test_entry(database_config)
