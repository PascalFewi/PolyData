import json
import time
import pandas as pd
import requests
from typing import Any, Dict, List
from src.models import Gamma_Market
from src.utils import logger, config, safe_float

GAMMA_MARKETS_BASE_URL = config["api"]["gamma_markets_base_url"]
CLOB_ORDERBOOK_BASE_URL = config["api"]["clob_orderbook_base_url"]
GAMMA_MARKETS_FETCH_RETRIES = config["api"]["gamma_markets_fetch_retries"]
CLOB_ORDERBOOK_FETCH_RETRIES = config["api"]["clob_orderbook_fetch_retries"]


def fetch_with_retries(url: str, retries: int = 2, backoff_factor: int = 2) -> Any:
    """
    Perform an HTTP GET request with retry and exponential backoff.

  Args:
        url (str): The URL to fetch.
        retries (int): Number of retry attempts (default is 2).
        backoff_factor (int): Backoff factor for exponential delays (default is 2).

    Returns:
        dict: The JSON response as a dictionary.

    Raises:
        requests.RequestException: If all retries fail.
    """
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=10)  # Add timeout
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt} failed for URL {url}: {e}")
            if attempt < retries:
                time.sleep(backoff_factor ** attempt)
            else:
                logger.error(f"All retries exhausted for URL {url}")
                raise


def btc_markets_from_gamma()-> List[Gamma_Market]:
    """
    Fetch all active markets from the GAMMA API, filtering for relevant tokens.

    Filters:
        - Includes markets with "bitcoin" in their slug.
        - Further filters based on keywords: "hit", "reach", "above".

    Returns:
        list[dict]: A list of filtered Gamma_Market objects containing:
            - id
            - slug
            - conditionId (Optional)
            - orderPriceMinTickSize (Optional)
            - orderMinSize (Optional)
            - clobTokenId

    Logs:
        INFO: Successful market fetch and filtering.
        ERROR: Failures during API calls or data parsing.
    """
    offset = 0
    results = []
    logger.info("Starting on GAMMA fetching active markets")

    while True:
        url = f"{GAMMA_MARKETS_BASE_URL}&offset={offset}"

        try:
            # Use the fetch_with_retries method for better stability
            data = fetch_with_retries(url, retries=5, backoff_factor=2)
        except Exception as e:
            logger.error(f"Failed to fetch data from GAMMA API after retries: {e}")
            return []

        if not data:
            break

        for market in data:
            results.append({
                "id": market.get("id"),
                "slug": market.get("slug"),
                "conditionId": market.get("conditionId"),
                "orderPriceMinTickSize": market.get("orderPriceMinTickSize"),
                "orderMinSize": market.get("orderMinSize"),
                "clobTokenIds": market.get("clobTokenIds"),
            })

        offset += 100
        time.sleep(0.2)
    logger.info(f"Successfully fetched {len(results)} markets")
    df = pd.DataFrame(results)
    filtered_df = df[df['slug'].str.contains("bitcoin", case=False, na=False)]
    keywords = ["hit", "reach", "above"]
    filtered_df = filtered_df[filtered_df['slug'].str.contains('|'.join(keywords), case=False, na=False)]
    filtered_df['clobTokenId'] = filtered_df['clobTokenIds'].apply(lambda x: int(json.loads(x)[0]))
    filtered_df = filtered_df.drop(columns=['clobTokenIds'])

    markets = [
        Gamma_Market(
            id=row['id'],
            slug=row['slug'],
            conditionId=str(row.get('conditionId') or ""),
            orderPriceMinTickSize=safe_float(row.get('orderPriceMinTickSize')),
            orderMinSize=safe_float(row.get('orderMinSize')),
            clobTokenId=row['clobTokenId']
        )
        for _, row in filtered_df.iterrows()
    ]
    return markets

def orderbook_from_clob(token_id: int)-> Any:
    """
    Fetch the order book for a given token ID from the CLOB API.

    Args:
        token_id (int): The unique token ID.

    Returns:
        dict: The JSON response containing the order book data.

    Logs:
        INFO: Successful order book retrieval.
        ERROR: Failures during API calls or retries.
    """
    url = f"{CLOB_ORDERBOOK_BASE_URL}token_id={token_id}"
    try:
        return fetch_with_retries(url, retries=CLOB_ORDERBOOK_FETCH_RETRIES, backoff_factor=2)
    except Exception as e:
        logger.error(f"Failed to fetch order book for token_id {token_id} after retries. Error: {e}")
        return None
