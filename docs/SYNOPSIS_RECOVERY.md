# Conservative recovery of cached film synopses

This stage targets only films currently marked `missing_plot_section`. It makes no HTTP requests, keeps all available synopses unchanged, and preserves biographies and effective film/person associations, including the cached-biography supplement.

```bash
docker compose exec -T spark python -m unittest tests.test_enrichment tests.test_synopsis_recovery -v
docker compose exec -T spark python -m jobs.enrichment.recover_synopses --run-id 20261114T142005Z
```

Use your own selected run ID for reproduction. Run one Spark application at a time. Cached target records are read on Spark workers by validated IMDb ID and parsed with an explicit Spark schema; no full directory scan or driver-side list of all article paths is required.

## Rules

- The recognized level-two headings include `Summary`, `Premise` and `Storyline`, in addition to `Plot`, `Synopsis`, `Plot synopsis`, `Plot summary` and `Story`.
- Paragraphs within an accepted section and its subsections are retained until the next level-two heading. Infoboxes, references, scripts and edit controls are excluded. The lead is never substituted for a film synopsis.
- Multiple nonempty recognized sections return `ambiguous_plot_sections`; their contents are not combined.
- Level-three-only synopsis headings and mixed headings such as `Production and synopsis` remain manual-review cases.
- The cached API page title must match the original linked English Wikipedia article title after URL decoding and conservative whitespace/Unicode/case normalization. A redirect to a different title returns `redirect_requires_review` in the recovery decisions, avoiding automatic extraction from a film-series page.
- Accepted text must come from the same revision as the previous snapshot. Wikipedia revision URLs, timestamps, licenses and every other film column are preserved.

## Publication and evidence

The job publishes an immutable snapshot after validating keys, unchanged existing synopses, unchanged nonrecovered films, biographies, biography coverage, source revision, and publication/selection guards. It updates only the synopsis and status of accepted target films. Unaccepted targets retain their previous published status; detailed recovery decisions explain rejection separately.

Evidence is under `results/enrichment/<run_id>/synopsis_recoveries/<snapshot>/`: coverage summary, prior publication/summary, and `decisions.csv`. Initial selections, raw articles and previous snapshots are retained. The new manifest preserves `film_people_path` and `supplemental_film_people_path`, so downstream readers continue to use the effective supplementary associations.

Rerunning the same extraction version returns `already_published` without creating another snapshot. The biography-supplement job also detects retained supplemental associations after synopsis recovery. A complete raw preparation creates a fresh base snapshot instead; the reproducible sequence is raw preparation → cached-biography supplementation → synopsis recovery.

Generated data and reports are excluded from Git. This delivery does not change `PROJECT.md`, its user-written AI declaration or `REPORT.md`.
