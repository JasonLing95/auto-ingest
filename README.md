## SEC 13F Filings Fetcher
A Python tool to fetch, parse, and store SEC 13F-HR filings for institutional investment managers using CIK numbers.

## Features
- Fetches latest 13F-HR and 13F-HR/A filings from the SEC Edgar database.
- Tracks changes to a CIK list file and dynamically reloads it.
- Parses XML filings and stores holdings data in a PostgreSQL database.
- Rate-limited to comply with SEC guidelines (10 requests/second).
- Includes healthcheck pings for monitoring.

## Setup
- Python Version 3.11.4
- PostgreSQL database
- SEC Edgar API

## Installation
Clone the repo
```
git clone https://github.com/JasonLing95/auto-ingest.git
cd auto-ingest
```

Install dependencies
```
pip install -r requirements.txt
```

Setup environments
```
SEC_IDENTITY="example.email@example.com"  # SEC requires this!
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="postgres"
DB_PASSWORD="yourpassword"
DB_NAME="sec"
CIK_FILE="ciks.txt" # Path to CIK list
HEALTHCHECK_URL="https://hc-ping.com/your-uuid" # Optional
```

## Usage
Run the script
```
python main.py --cik path/to/ciks.txt
```

Run it in Docker
```
docker run -d --network=common-net -v /app/logs:/app/logs -v /app/cik:/app/cik -e DB_HOST=sec-database -e DB_PASSWORD=password -e DB_PORT=5432 -e DB_NAME=sec autoingest:1.0 --cik /app/cik/ciks.txt
```

