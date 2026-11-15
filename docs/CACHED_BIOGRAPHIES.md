# Supplementing films with previously collected biographies

Run after full Wikipedia collection and Spark text preparation. This step needs no HTTP access and does not collect or rewrite text. It uses a verified available biography only when the cleaned `title.principals` table supplies a corresponding film/person credit.

```bash
docker compose exec -T spark python -m unittest tests.test_cached_biography_supplements -v
docker compose exec -T spark python -m jobs.enrichment.supplement_cached_biographies --run-id 20261114T142005Z
```

Use your own selected run ID when reproducing the project. The IMDb catalog must match the selection's source catalog. Run only one Spark application at a time.

## Selection and provenance

The original film rankings, original selected people, raw responses and biographies remain intact. For films with fewer than three available biographies, the job considers other IMDb principals whose biographies are already available in this run. It excludes existing film/person pairs, groups repeated roles for the same person, orders by minimum IMDb `ordering` then `nconst`, and adds only enough available people to reach three. A film can remain below three when insufficient eligible biographies exist.

The new `film_people` output retains the initial rows and identifies additions with `association_source = cached_biography_supplement`. Original `principal_rank` values are preserved; supplements have a null `principal_rank` and a separate `supplement_rank`. Each row records the evidence table, source catalog and the biography's Wikipedia revision URL when available. These are supplementary principal associations, not replacements for the original first-three selection.

## Publication and validation

The job writes an immutable snapshot with unchanged `films` and `people` text tables, merged `film_people`, `supplemental_film_people`, and updated `film_biography_coverage`. It checks the original associations, pair uniqueness, existing text provenance, unchanged film/person rows, recomputed coverage, no coverage regression, source catalog and concurrent-publication guards before publishing `current_content.json`.

The manifest's `film_people_path` identifies the effective associations. Consumers must use this path when present, falling back to `selection/film_people` for earlier snapshots. Joining only the initial selection after supplementation understates biography coverage.

The summary retains the original `selection_film_person_links`, reports the effective `film_person_links` and `supplemental_film_person_links`, and gives the gains in film coverage. The number of unique biographies does not increase through reuse. Immutable evidence is saved under `results/enrichment/<run_id>/supplements/<snapshot>/`; `coverage.json` is the convenient current summary. Earlier snapshots are preserved.

A rerun against an already supplemented publication returns `already_published` and does not add links or create another snapshot. Running the original `prepare_wikipedia` again publishes an unsupplemented snapshot from the raw collection; reapply this supplement afterward if that is intended.

Code and this guide belong in a feature PR. Generated Parquet files, metadata and coverage reports are excluded from Git. `PROJECT.md`, its user-written AI declaration and `REPORT.md` are not changed by this delivery.
