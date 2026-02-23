# ASX Insider Tracker

A FastAPI backend service for tracking insider director trades on the Australian Securities Exchange (ASX).

## Project Structure

```
asx-insider-tracker/
├── backend/                 # FastAPI application
│   ├── app/
│   │   ├── models/         # SQLAlchemy ORM models
│   │   ├── routers/        # API endpoint routers
│   │   ├── main.py         # FastAPI app entry point
│   │   ├── config.py       # Configuration management
│   │   └── database.py     # Database setup
│   ├── alembic/            # Database migrations
│   ├── requirements.txt    # Python dependencies
│   └── Dockerfile
├── frontend/               # Frontend application (placeholder)
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

### Verify Installation

```bash
curl http://localhost:8000/health
```

Expected response:
```json
{
  "status": "ok",
  "db": "connected",
  "timestamp": "2024-02-17T10:00:00.000000",
  "version": "0.1.0",
  "environment": "development"
}
```

## Database Models

### companies
- **id**: UUID primary key
- **ticker**: Unique stock ticker (e.g., "CBA", "NAB")
- **name**: Company name
- **sector**: GICS sector classification
- **industry_group**: Industry group classification
- **market_cap**: Market capitalization in dollars
- **is_asx200**: Boolean flag for ASX 200 membership
- **is_asx300**: Boolean flag for ASX 300 membership

### directors
- **id**: UUID primary key
- **full_name**: Director's full name

### director_companies (Junction Table)
- **id**: UUID primary key
- **director_id**: Foreign key to directors
- **company_id**: Foreign key to companies
- **role**: Director's role (e.g., "Managing Director", "CFO")
- **is_active**: Whether the director is currently active
- **smart_money_score_30d**: Smart money score (30-day window)
- **smart_money_score_60d**: Smart money score (60-day window)
- **smart_money_score_90d**: Smart money score (90-day window)
- **trade_count**: Number of trades by this director

### trades
- **id**: UUID primary key
- **director_id**: Foreign key to directors
- **company_id**: Foreign key to companies
- **date_of_trade**: Date the trade occurred
- **date_lodged**: Date the trade was lodged
- **trade_type**: Type of trade (on_market_buy, on_market_sell, off_market, exercise_options, other)
- **quantity**: Number of shares traded
- **price_per_share**: Price per share (if applicable)

### price_snapshots
- **id**: UUID primary key
- **ticker**: Stock ticker
- **date**: Date of the price snapshot
- **open**: Opening price
- **high**: High price
- **low**: Low price
- **close**: Closing price
- **volume**: Trading volume

### announcements
- **id**: UUID primary key
- **ticker**: Stock ticker
- **announcement_date**: Date of announcement
- **announcement_type**: Type of announcement (e.g., "Quarterly Report", "Director Change")
- **title**: Announcement title
- **url**: URL to the announcement
- **is_price_sensitive**: Whether announcement is price-sensitive

## API Endpoints

### Health Check
- **GET** `/health` - Health check endpoint
  - Returns: API status, database connection status, timestamp, version

### Root
- **GET** `/` - API information
  - Returns: API name, version, environment

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
# Create a new migration
alembic revision --autogenerate -m "Description"

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

## Environment Variables

See `.env.example` for all available environment variables.

Key variables:
- `DATABASE_URL`: PostgreSQL connection string
- `APP_ENV`: Application environment (development/production)
- `APP_VERSION`: Application version

## Testing

Health check endpoint returns:
```json
{
  "status": "ok",
  "db": "connected",
  "timestamp": "ISO 8601 timestamp",
  "version": "0.1.0",
  "environment": "development"
}
```

## Technology Stack

- **Framework**: FastAPI
- **ORM**: SQLAlchemy
- **Database**: PostgreSQL
- **Async**: asyncpg
- **Migrations**: Alembic
- **Server**: Uvicorn
- **Containerization**: Docker & Docker Compose

## Future Features

- Director and company data ingestion from ASX
- Trade analysis and smart money scoring
- Frontend dashboard
- WebSocket real-time updates
- REST API endpoints for querying data
