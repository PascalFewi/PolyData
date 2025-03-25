from typing import Any
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from src.models import DatabaseConfig, MetadataEntry
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Any
from src.models import DatabaseConfig
from src.utils import logger

def get_db_connection(database_config: DatabaseConfig) -> Any:
    """
    Establish a connection to the PostgreSQL database using SSL.
    Args:
        database_config (DatabaseConfig): The database configuration.
    Returns:
        A psycopg2 connection object.
    """
    try:
        conn = psycopg2.connect(
            dbname=database_config.DB_NAME,
            user=database_config.DB_USER,
            password=database_config.DB_PASSWORD,
            host=database_config.DB_HOST,
            port=database_config.DB_PORT,
            sslmode="require",
            cursor_factory=RealDictCursor
        )
        logger.debug("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        logger.critical(f"Error connecting to the database: {e}")
        raise



def insert_metadata(
        metadata: MetadataEntry,
        file_path: str,
        database_config: DatabaseConfig
        )-> None:
    """
    Insert a metadata entry into the orderbook_metadata table.

    Args:
        metadata (MetadataEntry): The metadata entry to insert.
        file_path (str): The file path of the uploaded orderbook.

    Notes:
        This function enforces a unique constraint on (market_id, date, hour).
        Duplicate entries for the same market and hour are ignored.
    """
    query = """
    INSERT INTO orderbook_metadata (
        market_id, hour, date, fetched_at, slug, condition_id, clob_token_id, start_time, 
        end_time, num_updates, order_price_min_tick_size, order_min_size, generated_at, file_path
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (market_id, date, hour) DO NOTHING;
    """
    conn = get_db_connection(database_config)
    try:
        
        with conn.cursor() as cur:
            cur.execute(query, (
                metadata.market_id,
                metadata.hour,
                metadata.date,
                metadata.fetched_at,
                metadata.slug,
                metadata.condition_id,
                metadata.clob_token_id,
                metadata.start_time,
                metadata.end_time,
                metadata.num_updates,
                metadata.order_price_min_tick_size,
                metadata.order_min_size,
                metadata.meta_generated_at,
                file_path
            ))
            conn.commit()
            logger.debug(f"Successful upload: {metadata.market_id}")
    except Exception as e:
        logger.critical(f"Failed to upload: {e}")
    finally:
        conn.close()
