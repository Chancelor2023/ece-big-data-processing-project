"""Collect Wikipedia article HTML, matched through exact Wikidata IMDb IDs."""

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from itertools import islice
from uuid import uuid4

from imdb_pipeline.config import ENRICHMENT_DIR, RESULTS_DIR
from imdb_pipeline.wikimedia import WikimediaClient, save_json

QUERY_ENDPOINT = "https://query.wikidata.org/sparql"
ARTICLE_ENDPOINT = "https://en.wikipedia.org/w/api.php"


def iter_requests(directory):
    for path in sorted(directory.glob("part-*.json")):
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                yield json.loads(line)


def lookup_query(ids):
    if not all(re.fullmatch(r"(?:tt|nm)[0-9]+", value) for value in ids):
        raise ValueError("Invalid IMDb identifier in collection request")
    values = " ".join(json.dumps(value) for value in ids)
    return f'''SELECT ?imdb ?item ?article ?title WHERE {{
        VALUES ?imdb {{ {values} }}
        ?item wdt:P345 ?imdb .
        OPTIONAL {{ ?article schema:about ?item;
            schema:isPartOf <https://en.wikipedia.org/>; schema:name ?title. }}
    }}'''


def reuse_cached(cache, destination, expected_id):
    """Restore missing output, without rewriting a committed existing record."""
    if not cache.exists():
        return None
    try:
        cached = json.loads(cache.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if cached.get("imdb_id") != expected_id or cached.get("status") in (None, "api_error"):
        return None
    if not destination.is_file() or destination.stat().st_size == 0:
        save_json(destination, cached)
    return cached["status"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id")
    parser.add_argument("--scope", choices=("pilot", "full"), default="pilot")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    run = args.run_id or json.loads((ENRICHMENT_DIR / "current_selection.json").read_text())["run_id"]
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z", run):
        parser.error("Invalid run identifier")
    root = ENRICHMENT_DIR / "runs" / run
    requested = root / "selection" / ("pilot_requests" if args.scope == "pilot" else "requests")
    if not requested.is_dir():
        raise ValueError(f"Run selection first: {requested}")
    client = WikimediaClient(ENRICHMENT_DIR / "cache" / "http", refresh=args.refresh)
    requests = iter(iter_requests(requested))
    counts, overall = Counter(), Counter()
    selection = json.loads((root / "selection_summary.json").read_text())
    total = selection["pilot_requests"] if args.scope == "pilot" else selection["unique_films"] + selection["unique_people"]
    started = datetime.now(timezone.utc).isoformat()
    session_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6]
    progress_path = RESULTS_DIR / "enrichment" / run / f"collection_{args.scope}.json"
    if progress_path.exists():
        save_json(progress_path.parent / "collection_history" / f"{args.scope}-{session_id}.json", json.loads(progress_path.read_text()))

    def report(status, phase, error=None):
        state = {
            "run_id": run, "scope": args.scope, "session_id": session_id,
            "status": status, "phase": phase, "started_at_utc": started,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "processed": sum(counts.values()), "total_targets": total,
            "counts": dict(counts), "overall_status_counts": dict(overall),
        }
        if error:
            state["error"] = error
        save_json(progress_path, state)
        print(json.dumps(state), flush=True)

    report("running", "restoring_cache")
    try:
        while batch := list(islice(requests, 50)):
            pending = []
            for item in batch:
                identifier = item["imdb_id"]
                cache = ENRICHMENT_DIR / "cache" / "records" / f"{identifier}.json"
                reused = reuse_cached(cache, root / "raw" / f"{identifier}.json", identifier) if not args.refresh else None
                if reused is not None:
                    counts["reused"] += 1
                    overall[reused] += 1
                else:
                    pending.append(item)
            report("running", "lookup" if pending else "restoring_cache")
            if not pending:
                continue
            try:
                lookup = client.get(QUERY_ENDPOINT, {"query": lookup_query([r["imdb_id"] for r in pending]), "format": "json"})
                bindings = json.loads(lookup["raw_response"])["results"]["bindings"]
                lookup_error = None
            except (RuntimeError, ValueError, KeyError) as error:
                lookup, bindings, lookup_error = None, [], str(error)
            for item in pending:
                identifier = item["imdb_id"]
                record = {
                    "imdb_id": identifier, "kind": item["kind"],
                    "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                    "lookup_url": lookup["url"] if lookup else QUERY_ENDPOINT,
                }
                matches = [row for row in bindings if row["imdb"]["value"] == identifier]
                entities = {row["item"]["value"] for row in matches}
                record["mapping_candidates"] = matches
                if lookup_error:
                    record.update(status="api_error", error=lookup_error)
                elif not matches:
                    record["status"] = "not_found"
                elif len(entities) != 1:
                    record["status"] = "ambiguous_match"
                else:
                    match = matches[0]
                    record["entity_uri"] = match["item"]["value"]
                    if "title" not in match:
                        record["status"] = "no_english_article"
                    else:
                        record["article_url"] = match["article"]["value"]
                        try:
                            article = client.get(ARTICLE_ENDPOINT, {
                                "action": "parse", "page": match["title"]["value"],
                                "prop": "text|revid", "redirects": "1", "format": "json",
                                "formatversion": "2", "maxlag": "5",
                            })
                            record.update(status="collected", raw_response=article["raw_response"], retrieval_url=article["url"], retrieved_at_utc=article["retrieved_at_utc"])
                        except (RuntimeError, ValueError) as error:
                            record.update(status="api_error", error=str(error))
                save_json(root / "raw" / f"{identifier}.json", record)
                save_json(ENRICHMENT_DIR / "cache" / "records" / f"{identifier}.json", record)
                counts[record["status"]] += 1
                overall[record["status"]] += 1
                print(f"{identifier} ({item['kind']}): {record['status']}", flush=True)
            report("running", "collecting")
        report("completed", "finished")
    except KeyboardInterrupt:
        report("interrupted", "stopped")
        print("Collection stopped; rerun the same scope/run ID to reuse saved records.", flush=True)
    except Exception as error:
        report("failed", "stopped", str(error))
        raise


if __name__ == "__main__":
    main()
