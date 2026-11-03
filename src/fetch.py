"""Download movie details (including cast and crew) from the TMDB API."""

import logging
import time

import requests

from src import config

logger = logging.getLogger(__name__)

# Temporary problems worth retrying: too many requests (429) and server errors (5xx)
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def fetch_movie(movie_id: int, session: requests.Session, api_key: str) -> dict | None:
    """Fetch one movie with its credits.

    Returns the movie as a dict, or None if it does not exist or cannot be fetched.
    Raises RuntimeError if TMDB rejects the API key, because every other call would fail too.
    """
    url = f"{config.TMDB_BASE_URL}/movie/{movie_id}"
    params = {"api_key": api_key, "append_to_response": "credits"}

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=config.REQUEST_TIMEOUT)
        except requests.RequestException as error:
            # Log only the error type: the full message contains the URL, which includes the API key
            logger.warning("Movie %s: %s on attempt %d/%d",
                           movie_id, type(error).__name__, attempt, config.MAX_RETRIES)
        else:
            if response.status_code == 200:
                movie = response.json()
                logger.info("Movie %s: fetched '%s'", movie_id, movie.get("title"))
                return movie
            if response.status_code == 401:
                raise RuntimeError("TMDB rejected the API key (401) - check TMDB_API_KEY in .env")
            if response.status_code == 404:
                logger.warning("Movie %s: not found (404) - skipped", movie_id)
                return None
            if response.status_code not in RETRY_STATUS_CODES:
                logger.error("Movie %s: unexpected HTTP %s - skipped", movie_id, response.status_code)
                return None
            logger.warning("Movie %s: HTTP %s on attempt %d/%d",
                           movie_id, response.status_code, attempt, config.MAX_RETRIES)

        if attempt < config.MAX_RETRIES:
            time.sleep(2 ** attempt)   # wait 2 s, then 4 s ("exponential backoff")

    logger.error("Movie %s: giving up after %d attempts", movie_id, config.MAX_RETRIES)
    return None