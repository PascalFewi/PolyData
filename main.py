
from datetime import datetime, timedelta, timezone
import os, queue, threading, time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from src.background_tasks import thread_background_file_sender, thread_background_market_fetcher
from src.fetcher import btc_markets_from_gamma
from src.models import DatabaseConfig, Gamma_Market, Order_Book, Orderbook_Track, SpacesConfig 
from src.orderbook import orderbook_fetch_and_add_updates, orderbook_initialize_orderbookTracks
from src.utils import config, logger

# ----- config ----- #

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

spaces_config: SpacesConfig  = SpacesConfig(
    **{
    "SPACES_ENDPOINT" : os.getenv("SPACES_ENDPOINT"),
    "SPACES_ACCESS_KEY" : os.getenv("SPACES_ACCESS_KEY"),
    "SPACES_SECRET_KEY" : os.getenv("SPACES_SECRET_KEY"),
    "SPACES_BUCKET_NAME" : os.getenv("SPACES_BUCKET_NAME"),
    "UPLOAD_WINDOW_S": config["spaces"]["upload_window_s"]
    }
)

database_config: DatabaseConfig  = DatabaseConfig(
    **{
        "DB_HOST": os.getenv("DB_HOST", ""),
        "DB_PORT": os.getenv("DB_PORT", ""),
        "DB_NAME": os.getenv("DB_NAME", ""),
        "DB_USER": os.getenv("DB_USER", ""),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", "")
    }
)




MARKET_FETCH_INTERVAL_MIN = config["intervals"]["market_fetch_interval_min"]
UPDATE_INTERVAL_S = config["intervals"]["update_interval_s"]

gamma_markets_queue: queue.LifoQueue[Any] = queue.LifoQueue()
file_uploading_queue: queue.LifoQueue[Any] = queue.LifoQueue()

# ----- crawler ----- #

def main() -> None:
    """
    Main loop to initialize, fetch, update, and upload order books at regular intervals.

    Logs:
        INFO: Logs server startup and periodic status updates.
        DEBUG: Logs detailed debug information about thread execution and updates.
        WARNING: Logs when no markets are found, and retry logic is triggered.
        CRITICAL: Logs fatal errors and restarts the script.

    Raises:
        Exception: If a fatal error occurs during the main execution loop.
    """
    now: datetime =datetime.now(timezone.utc)
    cycle_hour = (now.hour + 1 )%24

    logger.info("Starting Polymarket Server...")

    # Fetch initial markets
    gamma_markets: List[Gamma_Market] = btc_markets_from_gamma()
    if not gamma_markets:
        logger.warning("No markets found. Retrying in 60 seconds...")
        time.sleep(60)
        return main()  # Restart the main loop if no markets are found
    
    # Initialize order books and tracking
    current_orderbooks_track: Dict[str, Orderbook_Track]
    track_latest_orderbook: Dict[str, Order_Book]
    cycle_hour = now.hour 
    cycle_date = now.date().isoformat()
    current_orderbooks_track, track_latest_orderbook = orderbook_initialize_orderbookTracks(gamma_markets, cycle_hour, cycle_date)

    background_thread: Optional[threading.Thread] = None

    while True:
        now = datetime.now(timezone.utc)

        # Every UPDATE_INTERVAL_S seconds, fetch updates for each market
        if now.second % UPDATE_INTERVAL_S == 0:
            if now.minute % MARKET_FETCH_INTERVAL_MIN != 0:
                # Check if it's time to pre-fetch markets
                if (
                    now.minute % MARKET_FETCH_INTERVAL_MIN == (MARKET_FETCH_INTERVAL_MIN - 1)
                    and now.second == 30
                ):
                    if background_thread is None or not background_thread.is_alive():
                        logger.debug("Starting second thread for pre-fetching markets and saving order books.")
                        background_thread = threading.Thread(
                            target=thread_background_market_fetcher,
                            args=(current_orderbooks_track,gamma_markets_queue, file_uploading_queue)
                        )
                        background_thread.start()


                orderbook_fetch_and_add_updates(
                    gamma_markets,
                    current_orderbooks_track,
                    track_latest_orderbook
                    )

            else:
                # Refresh markets and reset order books at the start of a new cycle
                if now.second == 0:
                    if background_thread is not None:
                        background_thread.join()  # Ensure thread completion before proceeding
                        logger.debug("Fetcher-thread has completed.")
                        if not gamma_markets_queue.empty():
                            gamma_markets = gamma_markets_queue.get()

                    cycle_hour = now.hour 
                    cycle_date = now.date().isoformat()

                    current_orderbooks_track, track_latest_orderbook = orderbook_initialize_orderbookTracks(gamma_markets, cycle_hour, cycle_date)

                elif now.second == 30:
                    if background_thread is None or not background_thread.is_alive():
                        logger.debug("Starting sender thread.")
                        background_thread = threading.Thread(
                            target=thread_background_file_sender,
                            args=(file_uploading_queue,spaces_config, database_config))
                        background_thread.start()


                orderbook_fetch_and_add_updates(
                    gamma_markets,
                    current_orderbooks_track,
                    track_latest_orderbook
                )
        # Sleep briefly to avoid busy waiting
        time.sleep(0.5)

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            logger.critical(f"FATAL. Unexpected error: {e}. Restarting script in 60 seconds...")
            time.sleep(60)

