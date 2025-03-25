# Historical Data from Polymarket

A crawler to fetch and archive Polymarket order book data on DigitalOcean infrastructure. Designed for personal research use cases.

## Features

* Automated order book snapshots with configurable intervals
* Efficient sparse data storage format
* Cloud storage integration (DigitalOcean Spaces)
* PostgreSQL database support for metadata
* Type-checked codebase with unit tests

## Project Setup

#### Prerequisites

* Python 3.9+
* Docker (optional)
* DigitalOcean account with: 
  *  Spaces (Object Storage)
  * Managed PostgreSQL Database

#### Installation

```bash
pip install -r requirements.txt
pip install mypy
```

#### Configuration

* Create your [DigitalOcean](digitalocean.com) resources:
  * Spaces Bucket (Object Storage)
  * PostgreSQL Database
* Create .env file with your credentials:

```.env
SPACES_ENDPOINT=https://YOUR_SPACE.digitaloceanspaces.com
SPACES_ACCESS_KEY=your_access_key
SPACES_SECRET_KEY=your_secret_key
SPACES_BUCKET_NAME=orderbooks
DB_HOST=your-db.ondigitalocean.com
DB_PORT=25060
DB_NAME=polymarket
DB_USER=your_user
DB_PASSWORD=your_password
DB_SSLROOTCERT=/path/to/ca-certificate.crt
```


#### Optional Testing & Typing

```bash
pip install mypy

# Testing
python -m unittest discover -s tests

# Typing main
mypy main.py

# Typing Files
mypy src/
```

#### Build the Docker Image

To build the Docker image for the project:
```bash
docker build -t crawler .
```

#### Run the Docker

Start the application using Docker Compose in detached mode:

```bash
docker run --env-file .env crawler
```
For **debugging**, use the debug mode in config.json to run the application and view logs in real time.

Logging levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

To stop all running containers and clean up resources:

```bash
docker ps -a
docker stop <container_id>
```



### Sparse Data Format

The system uses a delta-based format to efficiently store order book changes.
To see how to download and reconstruct the data refer to the [Demo](DEMO_access_and_convert_data.ipynb).

```python
@dataclass
class OrderSummary:
    price: float  
    size: float   

@dataclass
class Order_Book:
    market: str
    asset_id: str
    fetched_at: str  # system date, when the orderbook was fetched (YYYY-MM-DD") 
    hash: str
    timestamp: str  #  polymarket date ("YYYY-MM-DD")
    bids: List[OrderSummary]
    asks: List[OrderSummary] 

@dataclass
class Changes:
    bids: List[OrderSummary] = field(default_factory=list)
    asks: List[OrderSummary] = field(default_factory=list)

@dataclass
class Updates:
    timestamp: str 
    changes: Changes

@dataclass
class Orderbook_Track:
    id: str
    slug: str
    start_orderbook: Order_Book
    updates: List[Updates] 
    ...

```


