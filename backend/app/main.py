"""FastAPI application entry point"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app import __version__
from app.config import settings
from app.database import close_db, init_db
from app.routers import (
    cluster_portfolio,
    clusters,
    compliance,
    contracts,
    health,
    jobs,
    portfolio_returns,
    public,
)
from app.scheduler import setup_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager for startup/shutdown events"""
    # Startup
    import subprocess
    import logging

    logger = logging.getLogger(__name__)

    # Run Alembic migrations
    logger.info("Running database migrations...")
    try:
        result = subprocess.run(
            ["alembic", "upgrade", "head"],
            cwd="/app",
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode != 0:
            logger.warning(f"Alembic migrations warning: {result.stderr}")
        else:
            logger.info("✓ Database migrations completed")
    except Exception as e:
        logger.error(f"Failed to run migrations: {e}")
        # Continue anyway - DB might already be up to date

    # Verify database connection
    await init_db()

    # Start the background scheduler for local/dev operation. Production can use
    # Railway Cron to trigger the same jobs without keeping an in-process timer.
    scheduler = None
    if settings.enable_in_app_scheduler:
        scheduler = setup_scheduler()
        scheduler.start()
    app.state.scheduler = scheduler

    yield

    # Shutdown
    if scheduler:
        scheduler.shutdown(wait=True)
    await close_db()


# Create FastAPI application
app = FastAPI(
    title="ASX Insider Tracker",
    description="Track insider director trades on the Australian Securities Exchange",
    version=__version__,
    lifespan=lifespan,
)

# Add CORS middleware (allow all origins for development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router)
app.include_router(compliance.router)
app.include_router(public.router)
app.include_router(clusters.router)
app.include_router(contracts.router)
app.include_router(cluster_portfolio.router)
app.include_router(portfolio_returns.router)
app.include_router(jobs.router)

# Mount static files
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
async def root():
    """Redirect root to the web UI"""
    return RedirectResponse(url="/static/index.html")


@app.get("/clusters")
async def clusters_page():
    return FileResponse(str(static_dir / "clusters.html"))


@app.get("/clusters/{cluster_id:int}")
async def cluster_detail_page(cluster_id: int):
    return FileResponse(str(static_dir / "cluster_detail.html"))


@app.get("/clusters/portfolio")
async def clusters_portfolio_page():
    return FileResponse(str(static_dir / "cluster_portfolio.html"))


@app.get("/cluster-portfolio")
async def legacy_cluster_portfolio_page():
    return RedirectResponse(url="/clusters/portfolio")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
    )
