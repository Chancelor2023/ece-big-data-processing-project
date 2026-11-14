"""Serial, cached Wikimedia HTTP access; no article text cleaning here."""

import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

USER_AGENT = os.environ.get(
    "WIKIMEDIA_USER_AGENT",
    "ECE-IMDb-Enrichment/1.0 (https://github.com/Chancelor2023/ece-big-data-processing-project)",
)


def save_json(path, value):
    path = Path(path)
    payload = json.dumps(value, ensure_ascii=False) + "\n"
    for attempt in range(4):
        temporary = None
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, filename = tempfile.mkstemp(
                prefix="." + path.name + ".", suffix=".tmp", dir=path.parent,
            )
            temporary = Path(filename)
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
                stream.write(payload)
            os.replace(temporary, path)
            return
        except (FileNotFoundError, PermissionError):
            if attempt == 3:
                raise
            time.sleep(0.25 * 2 ** attempt)
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass


def retry_delay(value, fallback):
    try:
        return max(0, float(value))
    except (ValueError, TypeError):
        try:
            return max(0, parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return fallback


class WikimediaClient:
    def __init__(self, cache_dir, refresh=False, interval=1.0):
        self.cache_dir = Path(cache_dir)
        self.refresh = refresh
        self.interval = max(1.0, interval)
        self.last_request = 0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    def get(self, endpoint, params):
        url = requests.Request("GET", endpoint, params=params).prepare().url
        key = hashlib.sha256(url.encode()).hexdigest()
        cache = self.cache_dir / f"{key}.json"
        if not self.refresh and cache.exists() and time.time() - cache.stat().st_mtime < 86400:
            return json.loads(cache.read_text(encoding="utf-8"))
        last_error = None
        for attempt in range(4):
            time.sleep(max(0, self.interval - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            response = None
            try:
                response = self.session.get(url, timeout=(10, 60))
                if response.status_code in (429, 500, 502, 503, 504):
                    raise requests.RequestException(f"HTTP {response.status_code}")
                response.raise_for_status()
                payload = response.json()
                if "error" in payload:
                    error = payload["error"]
                    if error.get("code") in ("maxlag", "ratelimited", "readonly"):
                        raise requests.RequestException(str(error))
                    raise ValueError(f"MediaWiki API error: {error}")
                result = {
                    "url": response.url, "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                    "raw_response": response.text,
                }
                save_json(cache, result)
                return result
            except requests.RequestException as error:
                last_error = error
                if response is not None and 400 <= response.status_code < 500 and response.status_code != 429:
                    break
                if attempt < 3:
                    delay = retry_delay(response.headers.get("Retry-After") if response is not None else None, max(5, 2 ** attempt))
                    # Honor the complete server delay, using short interruptible sleeps.
                    while delay > 0:
                        step = min(delay, 30)
                        time.sleep(step)
                        delay -= step
        raise RuntimeError(f"Wikimedia request failed: {url}: {last_error}")
