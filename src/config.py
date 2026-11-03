"""Project settings: folder paths, TMDB API details and the movie IDs to fetch."""

import os
from pathlib import Path

from dotenv import load_dotenv

# ---------- Paths (built from this file's location, so they work from anywhere) ----------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
RAW_MOVIES_FILE = RAW_DIR / "movies.json"
FIGURES_DIR = PROJECT_ROOT / "reports" / "figures"

# ---------- API settings ----------
load_dotenv(PROJECT_ROOT / ".env")

TMDB_BASE_URL = "https://api.themoviedb.org/3"
REQUEST_TIMEOUT = 10   # seconds to wait for TMDB before giving up
MAX_RETRIES = 3        # attempts per movie for temporary errors

# ---------- Movies to fetch (from the assignment; 0 is not a real movie) ----------
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397,
    420818, 24428, 168259, 99861, 284054, 12445,
    181808, 330457, 351286, 109445, 321612, 260513,
]


def get_api_key() -> str:
    """Return the TMDB API key from .env, or stop with a clear message if it is missing."""
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        raise RuntimeError("TMDB_API_KEY is missing - add it to the .env file in the project root")
    return api_key