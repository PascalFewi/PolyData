
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, TypedDict

@dataclass
class SpacesConfig: 
    SPACES_ENDPOINT: str
    SPACES_ACCESS_KEY: str
    SPACES_SECRET_KEY: str
    SPACES_BUCKET_NAME: str
    UPLOAD_WINDOW_S : int

@dataclass
class DatabaseConfig:
    DB_HOST: str
    DB_PORT: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str


@dataclass
class Gamma_Market:
    id: str
    slug: str
    conditionId: Optional[str]
    orderPriceMinTickSize: Optional[float]
    orderMinSize: Optional[float]
    clobTokenId: int


@dataclass
class OrderSummary:
    price: float  
    size: float   

@dataclass
class Order_Book:
    market: str
    asset_id: str
    fetched_at: str  # ISO 8601 format
    hash: str
    timestamp: str  # ISO 8601 format
    bids: List[OrderSummary]
    asks: List[OrderSummary] 

@dataclass
class Changes:
    bids: List[OrderSummary] = field(default_factory=list)
    asks: List[OrderSummary] = field(default_factory=list)

@dataclass
class Updates:
    timestamp: str # ISO 8601 format
    changes: Changes

@dataclass
class Orderbook_Track:
    id: str
    slug: str
    fetched_at: str  # ISO 8601 format; when the orderbook was initialized
    hour: int
    date: str  # ISO 8601 format (e.g., "YYYY-MM-DD")
    start_orderbook: Order_Book
    start_time_stamp: str  # ISO 8601 format; first timestamp from Clob
    condition_id: str
    order_price_min_tick_size: float
    order_min_size: float
    clob_token_id: int
    updates: List[Updates] 

@dataclass
class MetadataEntry:
    market_id: str
    hour: int
    date: str  # ISO 8601 format (e.g., "YYYY-MM-DD")
    fetched_at: str  # ISO 8601 format
    slug: str
    condition_id: str
    clob_token_id: str
    start_time: str  # ISO 8601 format
    end_time: str  # ISO 8601 format
    num_updates: int
    order_price_min_tick_size: float
    order_min_size: float
    meta_generated_at: str  # ISO 8601 format
