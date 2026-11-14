"""Collect Wikipedia article HTML, matched through exact Wikidata IMDb IDs."""

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from itertools import islice

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
    counts = Counter()
    while batch := list(islice(requests, 50)):
        pending = []
        for item in batch:
            cache = ENRICHMENT_DIR / "cache" / "records" / f"{item['imdb_id']}.json"
            if not args.refresh and cache.exists():
                cached = json.loads(cache.read_text(encoding="utf-8"))
                if cached["status"] != "api_error":
                    save_json(root / "raw" / f"{item['imdb_id']}.json", cached)
                    counts["reused"] += 1
                    continue
            pending.append(item)
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
            # Routing guards on an exact-ID API response; text remains untouched.
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
                        record.update(
                            status="collected", raw_response=article["raw_response"],
                            retrieval_url=article["url"], retrieved_at_utc=article["retrieved_at_utc"],
                        )
                    except (RuntimeError, ValueError) as error:
                        record.update(status="api_error", error=str(error))
            save_json(root / "raw" / f"{identifier}.json", record)
            save_json(ENRICHMENT_DIR / "cache" / "records" / f"{identifier}.json", record)
            counts[record["status"]] += 1
            print(f"{identifier} ({item['kind']}): {record['status']}", flush=True)
        save_json(RESULTS_DIR / "enrichment" / run / f"collection_{args.scope}.json", {
            "run_id": run, "scope": args.scope, "counts": dict(counts),
        })
    save_json(RESULTS_DIR / "enrichment" / run / f"collection_{args.scope}.json", {
        "run_id": run, "scope": args.scope, "counts": dict(counts),
    })
    print(json.dumps({"run_id": run, "scope": args.scope, "counts": dict(counts)}), flush=True)


if __name__ == "__main__":
    main()
