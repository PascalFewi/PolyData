

from datetime import datetime, timezone
import logging
from typing import Dict, List, Tuple

from src.fetcher import orderbook_from_clob
from src.models import Changes, Gamma_Market, Order_Book, OrderSummary, Orderbook_Track, Updates
from src.utils import logger, safe_float



def orderbook_initialize_orderbookTracks(
        gamma_markets: List[Gamma_Market],
        cycle_hour: int,
        cycle_date: str
        ) -> Tuple[Dict[str, Orderbook_Track], Dict[str, Order_Book]]:
    """
    Initialize the in-memory order books for all markets.

    Args:
        gamma_markets (List[Gamma_Market]): List of market objects containing metadata for each market.
        cycle_hour (int): The current hour cycle to associate with the initialized order book tracks.

    Returns:
        Tuple:
            - current_orderbooks_track (Dict[str, Orderbook_Track]): A dictionary of initialized order books for all markets,
              keyed by market ID, containing metadata and tracking details.
            - latest_orderbooks (Dict[str, Order_Book]): A dictionary of the latest snapshots of order books
              for tracking, keyed by market ID.

    Logs:
        DEBUG: Initialization progress for each market.
        INFO: Completion and details of markets initialized.
    """
    latest_orderbooks: Dict[str, Order_Book] = {}
    current_orderbooks_track: Dict[str, Orderbook_Track] = {}

    logger.debug("Refreshing markets and initializing order books.")
    log_string = ""

    for market in gamma_markets:
        log_string += market.slug + " / "
        initial_orderbook = orderbook_from_clob(market.clobTokenId)
        if initial_orderbook:
            # Convert clob timestamp (milliseconds) to ISO 8601 string
            clob_timestamp_iso = datetime.fromtimestamp(safe_float(initial_orderbook["timestamp"]) / 1000.0, tz=timezone.utc).isoformat()

            # Construct Order_Book object with ISO 8601 timestamps
            initial_orderbook = Order_Book(
                market=initial_orderbook["market"],
                asset_id=initial_orderbook["asset_id"],
                fetched_at=datetime.now(timezone.utc).isoformat(),  # Current time in ISO 8601
                hash=initial_orderbook["hash"],
                timestamp=clob_timestamp_iso,  # CLOB timestamp in ISO 8601
                bids=[OrderSummary(price=float(bid["price"]), size=float(bid["size"])) for bid in initial_orderbook["bids"]],
                asks=[OrderSummary(price=float(ask["price"]), size=float(ask["size"])) for ask in initial_orderbook["asks"]]
            )

            # Create the Orderbook_Track object
            current_orderbooks_track[market.id] = Orderbook_Track(
                id=market.id,
                slug=market.slug,
                fetched_at=initial_orderbook.fetched_at,  # Already ISO 8601
                hour=cycle_hour,
                date=cycle_date,
                start_orderbook=initial_orderbook,
                start_time_stamp=initial_orderbook.timestamp,  # Already ISO 8601
                condition_id=market.conditionId if market.conditionId else "",
                order_price_min_tick_size=market.orderPriceMinTickSize if market.orderPriceMinTickSize else 0.0,
                order_min_size=market.orderMinSize if market.orderMinSize else 0.0,
                clob_token_id=market.clobTokenId,
                updates=[]  # Empty updates initially
            )

            # Track the latest order book snapshot
            latest_orderbooks[market.id] = initial_orderbook

    logger.info(f"Refreshing done for: {log_string}")
    return current_orderbooks_track, latest_orderbooks



def orderbook_get_updates(
    old_orderbook: Order_Book,
    new_orderbook: Order_Book
) -> Updates:
    """
    Compute the differences between two order books.

    Args:
        old_orderbook (Order_Book): The previous order book data.
        new_orderbook (Order_Book): The latest order book data.

    Returns:
        Changes: An object containing the differences between the bid and ask levels.
            - bids: List of changes to bid levels.
            - asks: List of changes to ask levels.
    """
    changes = Changes()

    # Compare bids
    old_bids = {bid.price: bid.size for bid in old_orderbook.bids}
    new_bids = {bid.price: bid.size for bid in new_orderbook.bids}
    for price, size in new_bids.items():
        if price not in old_bids or old_bids[price] != size:
            changes.bids.append(OrderSummary(price=price, size=size))
    for price in old_bids.keys() - new_bids.keys():
        changes.bids.append(OrderSummary(price=price, size=0))  # Price level removed

    # Compare asks
    old_asks = {ask.price: ask.size for ask in old_orderbook.asks}
    new_asks = {ask.price: ask.size for ask in new_orderbook.asks}
    for price, size in new_asks.items():
        if price not in old_asks or old_asks[price] != size:
            changes.asks.append(OrderSummary(price=price, size=size))
    for price in old_asks.keys() - new_asks.keys():
        changes.asks.append(OrderSummary(price=price, size=0))  # Price level removed


    # sort bids and asks
    changes.bids.sort(key=lambda x: x.price, reverse=True)
    changes.asks.sort(key=lambda x: x.price, reverse=True)

    updates = Updates(new_orderbook.fetched_at,changes ) # ISO 8601 format
    return updates

def orderbook_fetch_and_add_updates(
    gamma_markets: List[Gamma_Market],
    current_orderbooks_track: Dict[str, Orderbook_Track],
    latest_orderbooks: Dict[str, Order_Book]
) -> None:
    """
    Fetch updates for all markets and apply changes to in-memory order books.

    Args:
        gamma_markets (List[Gamma_Market]): List of market metadata objects.
        current_orderbooks_track (Dict[str, Orderbook_Track]): Dictionary of in-memory order books keyed by market ID.
        latest_orderbooks (Dict[str, Order_Book]): Dictionary of latest order book snapshots for comparison,
            keyed by market ID.

    Logs:
        DEBUG: Update start and end.
        INFO: Changes applied to the in-memory order books.

    Side Effects:
        - Updates `current_orderbooks_track` with new changes and timestamps.
        - Updates `latest_orderbooks` with the latest order book state.
    """
    logger.debug(f"Updating Markets started.")

    for market in gamma_markets:
        market_id = market.id
        new_orderbook_data = orderbook_from_clob(market.clobTokenId)  

        if new_orderbook_data:
            # Convert raw data into Order_Book
            new_orderbook = Order_Book(
                market=new_orderbook_data["market"],
                asset_id=new_orderbook_data["asset_id"],
                fetched_at=datetime.now(timezone.utc).isoformat(),  # ISO 8601 format
                hash=new_orderbook_data["hash"],
                timestamp=datetime.fromtimestamp(safe_float(new_orderbook_data["timestamp"]) / 1000.0, tz=timezone.utc).isoformat(),  # ISO 8601 format
                bids=[OrderSummary(price=float(bid["price"]), size=float(bid["size"])) for bid in new_orderbook_data["bids"]],
                asks=[OrderSummary(price=float(ask["price"]), size=float(ask["size"])) for ask in new_orderbook_data["asks"]]
            )

            current_orderbook = latest_orderbooks[market_id]
            current_hash = current_orderbook.hash
            new_hash = new_orderbook.hash

            if current_hash != new_hash:
                # Calculate changes using a helper function
                updates = orderbook_get_updates(current_orderbook, new_orderbook)

                # Update the order book track with new changes
                current_orderbooks_track[market_id].updates.append(updates)

                # Update the latest order book snapshot
                latest_orderbooks[market_id] = new_orderbook
            
            else:
                current_orderbooks_track[market_id].updates.append(
                    Updates(new_orderbook.fetched_at, Changes())
                )

    logger.info(f"Updating Markets complete.")