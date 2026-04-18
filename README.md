# ASX Insider Tracker

A FastAPI backend service for tracking insider director trades on the Australian Securities Exchange (ASX).

## Project Structure

```
asx-insider-tracker/
├── backend/                 # FastAPI application
│   ├── app/
│   │   ├── models/         # SQLAlchemy ORM models
│   │   ├── routers/        # API endpoint routers
│   │   ├── static/         # Web UI (HTML/CSS/JS)
│   │   ├── scripts/        # Data import and scraping scripts
│   │   ├── main.py         # FastAPI app entry point
│   │   ├── config.py       # Configuration management
│   │   ├── database.py     # Database setup
│   │   └── scheduler.py    # Background job scheduler
│   ├── alembic/            # Database migrations
│   ├── requirements.txt    # Python dependencies
│   └── Dockerfile
├── docker-compose.yml      # Docker services orchestration
├── .env                    # Environment variables
└── .env.example           # Example environment variables
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.12+ (for local development)

### Running with Docker Compose

```bash
docker-compose up --build
```

This will:
1. Start PostgreSQL database (port 5432)
2. Build and run the FastAPI backend (port 8000)
3. Apply database migrations automatically

### Access the Application

- **Web UI**: http://localhost:8000/static/index.html
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

### Verify Installation

```bash
curl http://localhost:8000/health
```

Expected response:
```json
{
  "status": "ok",
  "db": "connected",
  "timestamp": "2026-03-01T10:00:00.000000",
  "version": "0.3.0",
  "environment": "development"
}
```

## Features

- **Public Web Interface**: Browse insider trades, directors, and compliance records
- **Daily Auto-Updates**: Automatically fetches and parses ASX announcements daily at 7 PM Sydney time
- **Compliance Tracking**: Identifies late trade disclosures (ASX Listing Rule 3.19A.2 - 5 business days)
- **Trade Analysis**: Filter and sort trades by date, company, director, and trade type
- **Smart Formatting**: Sub-cent price handling, currency formatting, date parsing

## Database Models

### companies
- **id**: UUID primary key
- **ticker**: Unique stock ticker (e.g., "CBA", "NAB")
- **name**: Company name
- **sector**: GICS sector classification
- **market_cap**: Market capitalization in dollars

### directors
- **id**: UUID primary key
- **full_name**: Director's full name

### director_companies (Junction Table)
- **director_id**: Foreign key to directors
- **company_id**: Foreign key to companies
- **smart_money_score**: Historical smart money score
- **trade_count**: Number of trades by this director

### trades
- **id**: UUID primary key
- **director_id**: Foreign key to directors
- **company_id**: Foreign key to companies
- **date_of_trade**: Date the trade occurred
- **date_lodged**: Date the trade was lodged
- **trade_type**: Type of trade (on_market_buy, on_market_sell, off_market, exercise_options, other)
- **quantity**: Number of shares traded
- **price_per_share**: Price per share

### price_snapshots
- **id**: UUID primary key
- **ticker**: Stock ticker
- **date**: Date of the price snapshot
- **open, high, low, close**: OHLC prices
- **volume**: Trading volume

### announcements
- **id**: UUID primary key
- **ticker**: Stock ticker
- **announcement_date**: Date of announcement
- **title**: Announcement title
- **url**: URL to the announcement PDF

## API Endpoints

### Public API
- **GET** `/api/search` - Search companies and directors
- **GET** `/api/company/{ticker}` - Get company profile with directors and trades
- **GET** `/api/director/{id}` - Get director profile with trades and compliance record
- **GET** `/api/compliance/violations` - Get all compliance violations

### Health
- **GET** `/health` - Health check (API status, DB connection, version)
- **GET** `/` - API information

## Development

### Local Setup

```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Running Migrations

```bash
cd backend
# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```

### Running the Server

```bash
cd backend
uvicorn app.main:app --reload
```

Server will be available at http://localhost:8000

## Data Import

To import historical trade data:

```bash
docker-compose exec backend python -m app.scripts.seed_from_json /app/data/asx_director_transactions.json
```

## Environment Variables

See `.env.example` for all available environment variables.

Key variables:
- `DATABASE_URL`: PostgreSQL connection string
- `APP_ENV`: Application environment (development/production)
- `APP_VERSION`: Application version
- `DEBUG`: Enable debug mode

## Cluster Portfolio

`Cluster Portfolio` is a simulated paper portfolio and rules-based strategy tracker for Basic Materials cluster buys. It does not connect to a broker, does not place trades, and `/portfolio` remains intentionally unused and returns `404`.

Key env vars:
- `CLUSTER_PORTFOLIO_ENABLED`
- `CLUSTER_PORTFOLIO_DRY_RUN`
- `CLUSTER_PORTFOLIO_EMAIL_ENABLED`
- `CLUSTER_PORTFOLIO_EMAIL_TO`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USERNAME`
- `SMTP_PASSWORD`
- `SMTP_FROM`

Run one cycle manually:

```bash
curl -X POST http://localhost:8000/api/cluster-portfolio/run \
  -H 'Content-Type: application/json' \
  -d '{"dry_run": true, "send_emails": false}'
```

Cluster Portfolio endpoints:
- `GET /api/cluster-portfolio/summary`
- `GET /api/cluster-portfolio/positions`
- `GET /api/cluster-portfolio/events`
- `POST /api/cluster-portfolio/run`

## Technology Stack

- **Framework**: FastAPI
- **ORM**: SQLAlchemy (async)
- **Database**: PostgreSQL
- **Async**: asyncpg
- **Migrations**: Alembic
- **Server**: Uvicorn
- **Scheduling**: APScheduler
- **Containerization**: Docker & Docker Compose
- **Frontend**: Vanilla JavaScript (no frameworks)

## Deployment

This application is designed for deployment on Railway.app. See Railway documentation for setup instructions.
