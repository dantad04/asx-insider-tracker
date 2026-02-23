"""FastAPI application entry point"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app import __version__
from app.config import settings
from app.database import close_db, init_db
from app.routers import health


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager for startup/shutdown events"""
    # Startup
    await init_db()
    yield
    # Shutdown
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


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint"""
    return {
        "message": "ASX Insider Tracker API",
        "version": __version__,
        "environment": settings.app_env,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
    )
