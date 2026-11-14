"""Verify the live exact-identifier lookup independently of the event stream."""

import json

from imdb_pipeline.config import CHECKPOINT_DIR, RESULTS_DIR
from imdb_pipeline.stream_matching import ArticleLookup
from jobs.preparation.prepare_titles import atomic_json


def main():
    lookup = ArticleLookup(CHECKPOINT_DIR / "streaming" / "lookup-verification")
    rows = lookup.resolve(["The Shawshank Redemption", "Morgan Freeman", "Paris"], RESULTS_DIR / "streaming" / "lookup-verification" / "responses")
    found = {(r["title"], r["imdb_id"]) for r in rows}
    if ("The Shawshank Redemption", "tt0111161") not in found or ("Morgan Freeman", "nm0000151") not in found:
        raise ValueError(f"Expected exact IMDb matches absent: {found}")
    if any(title == "Paris" for title, identifier in found):
        raise ValueError("A country/city article must not be confused with a film title")
    report = {"status": "passed", "matches": rows, "live_external_lookup": True}
    atomic_json(RESULTS_DIR / "streaming" / "lookup-verification" / "verification.json", report)
    print(json.dumps({"status": "passed", "identifiers": sorted(found)}), flush=True)


if __name__ == "__main__":
    main()
