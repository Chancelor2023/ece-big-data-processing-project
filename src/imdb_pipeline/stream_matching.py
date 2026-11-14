"""Exact Wikidata sitelink/IMDb lookup for bounded streaming microbatches."""

import hashlib
import json
import time
from datetime import datetime, timezone
from itertools import islice

import requests
from pyspark.sql import functions as F, types as T

from imdb_pipeline.recent_changes import STREAM_USER_AGENT
from jobs.preparation.prepare_titles import atomic_json

LOOKUP_SCHEMA = "title string, imdb_id string, entity_uri string, wikidata_article_url string, lookup_url string, lookup_at_utc string"


def title_query(titles):
    values = " ".join(json.dumps(title, ensure_ascii=False) + "@en" for title in titles)
    return f'''SELECT ?title ?article ?item ?imdb WHERE {{
        VALUES ?title {{ {values} }}
        ?article schema:name ?title; schema:isPartOf <https://en.wikipedia.org/>;
            schema:about ?item .
        ?item wdt:P345 ?imdb .
    }}'''


class ArticleLookup:
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": STREAM_USER_AGENT, "Accept": "application/sparql-results+json"})
        self.last_request = 0

    def resolve(self, titles, evidence_dir):
        rows, pending = [], []
        for title in titles:
            path = self.cache_dir / (hashlib.sha256(title.encode()).hexdigest() + ".json")
            if path.exists() and time.time() - path.stat().st_mtime < 300:
                rows.extend(json.loads(path.read_text())["rows"])
            else:
                pending.append(title)
        iterator = iter(pending)
        while group := list(islice(iterator, 40)):
            error = None
            for attempt in range(4):
                time.sleep(max(0, 1 - (time.monotonic() - self.last_request)))
                self.last_request = time.monotonic()
                response = None
                try:
                    response = self.session.get("https://query.wikidata.org/sparql", params={"query": title_query(group), "format": "json"}, timeout=(10, 60))
                    response.raise_for_status()
                    payload = response.json()
                    bindings = payload["results"]["bindings"]
                    break
                except (requests.RequestException, ValueError, KeyError) as exc:
                    error = exc
                    if response is not None and response.status_code in (400, 401, 403, 404):
                        raise RuntimeError(f"Wikidata lookup refused: {exc}") from exc
                    if attempt == 3:
                        raise RuntimeError(f"Wikidata unavailable; microbatch not committed: {error}") from exc
                    delay = 5 * (attempt + 1)
                    if response is not None:
                        try:
                            delay = max(delay, int(response.headers.get("Retry-After", "0")))
                        except ValueError:
                            pass
                    while delay > 0:
                        step = min(delay, 30)
                        time.sleep(step)
                        delay -= step
            retrieved = datetime.now(timezone.utc).isoformat()
            atomic_json(evidence_dir / (hashlib.sha256(response.url.encode()).hexdigest() + ".json"), {
                "url": response.url, "retrieved_at_utc": retrieved, "response": payload,
            })
            for title in group:
                found = [{
                    "title": title, "imdb_id": r["imdb"]["value"],
                    "entity_uri": r["item"]["value"],
                    "wikidata_article_url": r["article"]["value"],
                    "lookup_url": response.url, "lookup_at_utc": retrieved,
                } for r in bindings if r["title"]["value"] == title]
                path = self.cache_dir / (hashlib.sha256(title.encode()).hexdigest() + ".json")
                atomic_json(path, {"rows": found})
                rows.extend(found)
        return rows


def match_identifiers(events, mapping, imdb_keys):
    valid = mapping.filter(F.col("imdb_id").rlike(r"^(tt|nm)[0-9]+$")).dropDuplicates(["title", "imdb_id"])
    known = imdb_keys.join(F.broadcast(valid), "imdb_id", "inner")
    return events.join(F.broadcast(known), "title", "inner")
