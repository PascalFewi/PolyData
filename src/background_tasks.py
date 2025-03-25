from datetime import datetime, timezone
from queue import LifoQueue
import time
from typing import Any, Dict
from src.fetcher import btc_markets_from_gamma
from src.models import DatabaseConfig, Orderbook_Track, SpacesConfig
from src.spaces import process_and_upload_orderbooks
from src.utils import logger


def thread_enqueue_all_orderbooks(
        current_orderbooks_track: Dict[str, Orderbook_Track], file_uploading_queue: LifoQueue[Any]
        ) -> None:
    """
    Enqueue all order books into the file-uploading queue.

    Args:
        - current_orderbooks_track (Dict[str, Orderbook_Track]): 
            Dictionary of order books to enqueue, keyed by market ID.
        - file_uploading_queue (Queue) with files that are for upload

    Side Effects:
        - Adds each order book to the `file_uploading_queue`.
        - Clears the in-memory `current_orderbooks_track` dictionary after enqueueing.

    Logs:
        DEBUG: Logs the enqueuing progress for each market ID.
    """
    # Iterate over the order books and enqueue them
    for market_id, orderbook_track in current_orderbooks_track.items():
        logger.debug(f"Enqueuing order book for market ID: {market_id}")
        file_uploading_queue.put({market_id: orderbook_track})

    # Clear the in-memory dictionary to prepare for the next cycle
    current_orderbooks_track.clear()
    logger.debug("Finished enqueuing all order books. Cleared in-memory storage.")

def thread_background_market_fetcher(
    current_orderbooks_track: Dict[str, Orderbook_Track], 
    gamma_markets_queue: LifoQueue[Any], 
    file_uploading_queue: LifoQueue[Any], 
) -> None:
    """
    Background thread to pre-fetch active markets and enqueue order books.

    Args:
        - current_orderbooks_track (Dict[str, Orderbook_Track]): Current in-memory state of order books.
        - gamma_markets_queue (LifoQueue): new BTC related markets for the next cycle
        - cycle_hour int: this is the hour [0,23] for which the orderbook is fetched. It will be used for the naming.

    Side Effects:
        - Updates `gamma_markets_queue` with pre-fetched markets.
        - Enqueues finalized order books into the `file_uploading_queue`.

    Logs:
        DEBUG: Thread activity and progress.
        INFO: Markets pre-fetched and order books enqueued.
    """
    logger.debug("Fetcher-thread started")

    # Pre-fetch markets
    new_gamma_markets = btc_markets_from_gamma()
    if not new_gamma_markets:
        logger.warning("No markets fetched from GAMMA API.")
        return

    logger.debug(f"Pre-fetched {len(new_gamma_markets)} active markets on GAMMA")

    # Wait until 57 seconds to ensure the final update occurs
    while datetime.now(timezone.utc).second < 57:
        time.sleep(0.5) 

    # Create a snapshot of the final state of order books
    snapshot_orderbooks = current_orderbooks_track.copy()
    gamma_markets_queue.put(new_gamma_markets)
    logger.info(f"Enqueued {len(new_gamma_markets)} markets for the next iteration.")

    # Save the snapshot of the order books
    thread_enqueue_all_orderbooks(snapshot_orderbooks, file_uploading_queue)
    logger.debug("Order books for upload have been enqueued.")



def thread_background_file_sender(file_uploading_queue: LifoQueue[Any], spaces_config: SpacesConfig, database_config: DatabaseConfig ) -> None:
    """
    Background thread to process and upload order books to DigitalOcean Spaces.

    Logs:
        DEBUG: Thread activity and progress.
        INFO: Completion status of uploads.
        ERROR: Any issues during the upload process.
    """
    logger.debug("Spaces-thread started")
    try:
        database_metadata_list = process_and_upload_orderbooks(file_uploading_queue, spaces_config, database_config)
        logger.info(f"Successfully processed and uploaded {len(database_metadata_list)} orderbooks.")
    except Exception as e:
        logger.critical(f"Error occurred during background file sending: {e}")
    finally:
        logger.debug("Spaces-thread completed")
