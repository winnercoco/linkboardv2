import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from typing import Optional

from scripts.metadata_scraper import (
    DATA_DIR,
    STREAM_CACHE_PATH,
    load_json,
    save_json,
    is_stream_cache_fresh,
    refresh_stream_only,
)


COOKIE_FILE = None  # e.g. str(DATA_DIR / "cookies.txt")


def get_cached_stream(page_url: str) -> Optional[dict]:
    stream_cache = load_json(STREAM_CACHE_PATH, {})
    record = stream_cache.get(page_url)

    if record and is_stream_cache_fresh(record):
        return record

    return None


def refresh_stream(page_url: str) -> Optional[dict]:
    stream_cache = load_json(STREAM_CACHE_PATH, {})

    record = asyncio.run(refresh_stream_only(page_url, COOKIE_FILE))

    if record:
        stream_cache[page_url] = asdict(record)
        save_json(STREAM_CACHE_PATH, stream_cache)
        return stream_cache[page_url]

    return None


def resolve_stream(page_url: str) -> Optional[dict]:
    record = get_cached_stream(page_url)

    if record:
        return record

    return refresh_stream(page_url)