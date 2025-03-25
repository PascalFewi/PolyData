import os
import json
from datetime import datetime, timedelta, timezone
from typing import Any, List, Tuple, Dict
import boto3
import time
from botocore.client import Config
from queue import LifoQueue
from src.database import get_db_connection, insert_metadata
from src.utils import config, logger
from src.models import DatabaseConfig, MetadataEntry, Orderbook_Track, SpacesConfig



SPACES_CONNECTION_RETRIES = config["spaces"]["connection_retries"]
BACKOFF_FACTOR = config["spaces"]["backoff_factor"]
FILE_STORAGE_DIR = config["files"]["storage_dir"]

def spaces_establish_connection(
    endpoint_url: str ,
    access_key: str ,
    secret_key: str ,
    retries: int = 5,
    backoff_factor: int = 2
) -> boto3.client:
    """
    Establish a connection to DigitalOcean Spaces with retries and exponential backoff.
    """
    logger.debug("Attempting to establish connection to DigitalOcean Spaces.")

    for attempt in range(1, retries + 1):
        try:
            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version="s3v4"),
            )
            logger.debug(f"Successfully established connection to Spaces on attempt {attempt}.")
            return client
        except Exception as e:
            logger.error(f"Attempt {attempt} failed to connect to Spaces: {e}")
            if attempt < retries:
                sleep_time = backoff_factor ** attempt
                logger.debug(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.critical("Failed to establish connection to Spaces after all retries.")
                raise

def spaces_prepare_metadata_entry(market_id: str, orderbook_track: Orderbook_Track) -> MetadataEntry:
    """
    Prepare metadata for a given order book.

    Args:
        market_id (str): The market ID associated with the order book.
        orderbook_track (Orderbook_Track): The in-memory tracking object for the order book.

    Returns:
        MetadataEntry: An object containing all the metadata required for the order book.

    Notes:
        - All timestamps are stored in ISO 8601 format for consistency.
        - If no updates exist, `end_time` will be set to an empty string.
    """
    # Ensure timestamps are in ISO 8601 format
    end_time = orderbook_track.updates[-1].timestamp if len(orderbook_track.updates)!=0 else orderbook_track.start_time_stamp

    return MetadataEntry(
        market_id=market_id,
        slug=orderbook_track.slug,
        hour= orderbook_track.hour,
        date = orderbook_track.date,
        fetched_at=orderbook_track.fetched_at, # Already ISO 8601
        condition_id=orderbook_track.condition_id,
        clob_token_id=orderbook_track.start_orderbook.asset_id,
        start_time=orderbook_track.start_time_stamp, # Already ISO 8601
        end_time=end_time, # Already ISO 8601
        num_updates=len(orderbook_track.updates),
        order_price_min_tick_size=orderbook_track.order_price_min_tick_size,
        order_min_size=orderbook_track.order_min_size,
        meta_generated_at=datetime.now(timezone.utc).isoformat(), # Current time in ISO 8601
    )

def spaces_upload_orderbook(
    market_id: str,
    orderbook_track: Orderbook_Track,
    metadata_entry: MetadataEntry,
    spaces_client: boto3.client,
    SPACES_BUCKET_NAME: str
) -> str:
    """
    Upload an order book to DigitalOcean Spaces.
    """
    logger.debug(f"Preparing order book upload for market_id: {market_id}")

    # Reorder fields for the JSON structure
    ordered_data = {
        "id": orderbook_track.id,
        "slug": orderbook_track.slug,
        "initial_orderbook_fetched_at": orderbook_track.fetched_at,
        "hour": orderbook_track.hour,
        "date": orderbook_track.date,
        "condition_id": orderbook_track.condition_id,
        "clob_token_id": orderbook_track.clob_token_id,
        "order_price_min_tick_size": orderbook_track.order_price_min_tick_size,
        "order_min_size": orderbook_track.order_min_size,
        "start_time_stamp": orderbook_track.start_time_stamp,
        "end_time_stamp": metadata_entry.end_time,
        "num_updates": metadata_entry.num_updates,
        "object_generated_at": metadata_entry.meta_generated_at,
        "start_orderbook": {
            "market": orderbook_track.start_orderbook.market,
            "timestamp": orderbook_track.start_orderbook.timestamp,
            "bids": [bid.__dict__ for bid in orderbook_track.start_orderbook.bids],
            "asks": [ask.__dict__ for ask in orderbook_track.start_orderbook.asks]
        },
        "updates": [
        {
            "timestamp": update.timestamp,
            "changes": {
                "bids": [change.__dict__ for change in update.changes.bids],
                "asks": [change.__dict__ for change in update.changes.asks]
            }
        }
        for update in orderbook_track.updates
        ]
    }

    # Prepare file paths

    filename = f"{orderbook_track.id}-{orderbook_track.date}-{orderbook_track.hour}.json"
    local_file_path = os.path.join(FILE_STORAGE_DIR, f"hourly/{market_id}/{filename}")
    remote_file_path = f"orderbooks/hourly/{market_id}/{filename}"

    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
    try:
        with open(local_file_path, "w") as f:
            json.dump(ordered_data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save order book locally for market_id: {market_id}. Error: {e}")
        raise
    
    # Upload to Spaces with retries
    retries = 5
    for attempt in range(retries):
        try:
            if not os.path.exists(local_file_path):
                logger.error(f"Local file does not exist: {local_file_path}")
            else:
                logger.debug(f"Local file path is valid: {local_file_path}")
            spaces_client.upload_file(local_file_path, SPACES_BUCKET_NAME, remote_file_path)
            logger.debug(f"Successfully uploaded {local_file_path} to {remote_file_path} in Spaces.")
            try:
                os.remove(local_file_path)
                logger.debug(f"Deleted local file: {local_file_path}")
            except Exception as e:
                logger.error(f"Failed to delete local file {local_file_path}. Error: {e}")
            break
        except Exception as e:
            logger.error(f"Upload attempt {attempt + 1} failed for {local_file_path}. Error: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.critical(f"All upload attempts failed for {local_file_path}.")
                raise
    return remote_file_path

def process_and_upload_orderbooks(
        file_uploading_queue: LifoQueue[Any], 
        spaces_config: SpacesConfig, 
        database_config: DatabaseConfig
        ) -> List[Tuple[MetadataEntry, str]]:
    """
    Process and upload all order books from the queue.

    Returns:
        List[Tuple[MetadataEntry, str]]: List of tuples containing metadata and file paths for FAILED uploaded files.

    Logs:
        INFO: Upload completion.
        ERROR: Issues during processing or upload.
    """
    database_metadata_list: List[Tuple[MetadataEntry, str]] = []

    spaces_client = spaces_establish_connection(
        access_key=spaces_config.SPACES_ACCESS_KEY,
        secret_key=spaces_config.SPACES_SECRET_KEY,
        endpoint_url=spaces_config.SPACES_ENDPOINT
)
    # control upload time
    upload_start_time = datetime.now(timezone.utc)
    upload_end_time = upload_start_time + timedelta(seconds=spaces_config.UPLOAD_WINDOW_S)

    while not file_uploading_queue.empty() and datetime.now(timezone.utc) < upload_end_time:
        item: Dict[str, Orderbook_Track] = file_uploading_queue.get()
        for market_id, orderbook_track in item.items():
            metadata_entry = spaces_prepare_metadata_entry(market_id, orderbook_track)
            try:
                spaces_filepath = spaces_upload_orderbook(
                    market_id, orderbook_track, metadata_entry, spaces_client,
                    SPACES_BUCKET_NAME=spaces_config.SPACES_BUCKET_NAME
                    )
                insert_metadata(metadata_entry, spaces_filepath, database_config)
                database_metadata_list.append((metadata_entry, spaces_filepath))
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Failed to process or upload orderbook for market_id {market_id}. Error: {e}")
                file_uploading_queue.put({market_id: orderbook_track})
   

    logger.error(f"Upload Queue left with {file_uploading_queue.qsize()} files.")
    spaces_client.close()
    return database_metadata_list