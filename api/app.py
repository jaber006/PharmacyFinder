"""
PharmacyFinder FastAPI Application
Interactive dashboard for pharmacy opportunity evaluation.
"""
import os
import sqlite3
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.routes import evaluate, sites, watchlist

# Database path
DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "pharmacy_finder.db",
)


def get_db() -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: verify DB exists
    if not os.path.exists(DB_PATH):
        raise RuntimeError(f"Database not found: {DB_PATH}")
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM v2_results")
    count = cur.fetchone()[0]
    conn.close()
    print(f"[PharmacyFinder] Connected to {DB_PATH} — {count} sites loaded")
    yield


app = FastAPI(
    title="PharmacyFinder",
    description="Interactive pharmacy opportunity dashboard",
    version="3.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(evaluate.router, prefix="/api")
app.include_router(sites.router, prefix="/api")
app.include_router(watchlist.router, prefix="/api")

# Static files (serve dashboard)
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
