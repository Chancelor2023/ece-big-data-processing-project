# Film and person enrichment

This stage selects IMDb targets with Spark, collects existing English Wikipedia articles, and cleans and joins their text with Spark. It does not generate synopsis or biography text. `PROJECT.md` and its user-written AI declaration are unchanged by this delivery; this file supplies the additional execution commands.

## Selection contract

- Years: 1980 through 2025 inclusive. Feature films use IMDb's `titleType = movie` classification; no extra runtime threshold is imposed, and unknown runtimes are retained.
- Ranking: `numVotes DESC`, then `averageRating DESC NULLS LAST`, then `tconst ASC`. A rating row with positive votes and a known genre is needed for this ranking.
- Exactly up to ten films per observed genre/year group, using `row_number`. A group with fewer than ten eligible titles retains all of them. Missing genres cannot be ranked into a group.
- `film_rankings` keeps each genre/year/rank membership. `films` deduplicates the film IDs across these memberships so collection occurs once per film.
- For each film, select up to three distinct people from `title.principals`, ordered by their minimum IMDb `ordering`, then `nconst`. Multiple roles by one person do not count as multiple people. The global person collection is also deduplicated across films.
- `films_with_fewer_than_three_people` lists films for which IMDb supplies insufficient distinct principals. These remain in scope with an explicit outstanding coverage issue.

Use the [read-only principal-shortfall audit](PRINCIPAL_SHORTFALLS.md) to compare source credits, quarantines, crews and supplementary Wikidata evidence without changing an active collection's inputs.

The ranking by votes was confirmed by Jerry. Definitions and methodological reasoning for the course report remain the group's own work.

## Run the pilot

Start Compose and prepare the seven IMDb tables first. From the project directory:

```bash
docker compose exec -T spark python -m unittest tests.test_enrichment -v
docker compose exec -T spark python -m jobs.enrichment.select_targets
docker compose exec -T spark python -m jobs.enrichment.collect_wikipedia
docker compose exec -T spark python -m jobs.enrichment.prepare_wikipedia
```

Selection covers the full target scope. Collection defaults to a pilot of three films with the highest vote counts and their selected people. Use `select_targets --pilot-films 10` to create a new full selection with a larger pilot. A new selection writes a new run directory; it does not replace previous collected runs.

`data/enrichment/current_selection.json` gives the selected run ID. Each run lives in `data/enrichment/runs/<run_id>/`. The clean publication inside that run is `current_content.json`. Selection and coverage summaries are in `results/enrichment/<run_id>/`.

## Continue collection on the full scope

To reuse the same selection and the already collected pilot records:

```bash
docker compose exec -T spark python -m jobs.enrichment.collect_wikipedia --scope full
docker compose exec -T spark python -m jobs.enrichment.prepare_wikipedia
```

Use `--run-id YYYYMMDDTHHMMSSZ` with either collection/preparation command to target a specific earlier run. Collection is serial and may take several hours on the full scope. Each record is saved as it completes. Rerunning collection reuses recorded successes and missing/ambiguous matches; failed API requests are retried. `--refresh` re-fetches records and bypasses the HTTP cache. Do not run simultaneous collectors against the same cache/run.

On resume, `restoring_cache` means saved records are being reused, without rewriting existing nonempty raw files. Progress is printed and saved after every batch, including batches entirely served from cache. In `results/enrichment/<run_id>/collection_full.json`, `processed` includes reused records, `counts` describes the current attempt and `overall_status_counts` includes the underlying statuses of reused records. The previous attempt's summary is archived in `collection_history/`. `completed` means the request list has been traversed; check API errors and content coverage separately.

An interrupt records `interrupted` when the process can handle it. Resume with the same scope and run ID after restarting Docker. File writes use unique temporary files and retry transient missing-path/permission errors; preparation reads only completed `*.json` records. Sleeping the computer still suspends network access and is not a background execution option.

### View progress on Windows

Double-click `scripts/Ouvrir-Suivi-Collecte.cmd`, or run the following from a second PowerShell window in the project directory:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Watch-Enrichment.ps1
```

The display refreshes every ten seconds. Add `-Once` for a single snapshot, or `-RunId 20261114T142005Z` to pin an existing run instead of following the current selection at launch. The viewer only reads `results/enrichment/<run_id>/collection_full.json`; it does not launch, stop or modify collection. Closing the viewer has no effect on the collector.

The underlying report is updated by batches of fifty, so unchanged counters between refreshes are expected. The viewer shows the last checkpoint's age and warns after five minutes without a recent checkpoint; a report declaring `running` alone cannot establish whether the process is still alive. Collected HTML counts are separate from final Spark-cleaned text coverage.

Requests use an identifying User-Agent, a minimum one-second interval, bounded retries, `Retry-After` delays and MediaWiki `maxlag=5`. HTTP responses are cached for 24 hours; per-entity snapshots persist until explicitly refreshed. Set `WIKIMEDIA_USER_AGENT` for a different identifying contact string. These behaviors follow [MediaWiki API etiquette](https://www.mediawiki.org/wiki/API:Etiquette) and [maxlag guidance](https://www.mediawiki.org/wiki/Manual:Maxlag_parameter). They remain subject to server-imposed throttling.

## Matching, text and lineage

Wikidata's query service resolves the exact IMDb identifier through property `P345` and the English Wikipedia sitelink. No fuzzy name matching is used. Multiple matching Wikidata entities are marked ambiguous. An absent match or English article is recorded explicitly; no secondary text source is queried.

The collector stores the API's original JSON/HTML, lookup URL, entity URI, article URL, request URL and retrieval timestamp. It performs HTTP routing and checkpointing, without cleaning article text. The Spark job enforces the raw schema, runs an HTML parser as a Spark UDF, and joins the results to the full target set.

Film synopses contain the paragraphs of a `Plot`, `Synopsis`, `Plot synopsis`, `Plot summary` or `Story` section, including its subsections. A general film introduction is not substituted when that section is missing. Person biographies contain the article's introductory paragraphs before the first level-two heading. Infobox tables, scripts, superscript references and edit controls are excluded during Spark processing. Text is copied and normalized, not rewritten or summarized by an LLM.

Each collected text retains the Wikipedia page ID, revision ID, revision permalink (`source_url`), retrieval timestamp, Wikidata URI and [Wikipedia's CC BY-SA 4.0 attribution link](https://creativecommons.org/licenses/by-sa/4.0/). Retain this lineage when using or exporting text.

## Outputs and coverage

The current clean snapshot contains:

| Output | Key | Added content |
| --- | --- | --- |
| `films` | `tconst` | `synopsis`, status and lineage |
| `people` | `nconst` | `biography`, status and lineage |
| `film_biography_coverage` | `tconst` | Count of distinct selected people with available biographies |

Statuses include `available`, `pending`, `not_found`, `ambiguous_match`, `no_english_article`, `missing_plot_section`, `missing_biography_intro`, `api_error` and `missing_article_html`. Missing records keep their programmatic status; text is never fabricated.

The outputs retain the full selection even during a pilot. Consequently, most rows initially have `pending` status. `collection_attempted_for_all_targets` excludes pending requests and API failures; it does not assert that text was found. `full_enrichment_complete` requires available text for every target and at least three available biographies per film. Legitimate missing pages and the insufficient-principal list must be examined and reported before submission, even if the API traversal is finished.

```python
import json
from imdb_pipeline.config import ENRICHMENT_DIR

run = json.loads((ENRICHMENT_DIR / "current_selection.json").read_text())["run_id"]
root = ENRICHMENT_DIR / "runs" / run
publication = json.loads((root / "current_content.json").read_text())
films = spark.read.parquet(str(root / publication["path"] / "films"))
people = spark.read.parquet(str(root / publication["path"] / "people"))
```

Raw articles, cache files, Parquet outputs and summaries are ignored by Git. Commit the code and this guide; peers reproduce the data with the commands above. This branch depends on the seven-table preparation delivery until that PR is merged.
