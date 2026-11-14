# Auditing films with fewer than three principals

This read-only diagnostic compares the selected people with all valid credits in `title.principals`, the original orphan quarantine, `title.crew`, and optional structured Wikidata credit evidence. It does not alter the active collection's selection, requests, cleaned IMDb tables or cached article records.

Run from the project directory after selecting enrichment targets:

```bash
docker compose exec -T spark python -m jobs.enrichment.audit_principal_shortfalls --wikidata
```

Omit `--wikidata` for an IMDb-only audit. Each execution has its own output directory. `results/enrichment/<selection_run>/current_principal_audit.json` points to the most recent completed audit. Failed attempts do not replace this pointer.

## Checks and scope

- Recount the films with fewer than three distinct selected people and compare this count with the selection summary.
- Compare selected counts with all valid distinct principals for each film, including every credit category and ordering.
- Reconstruct source-principal counts using valid credits and scalar-orphan quarantines. This reconstruction requires that the preparation quality report record no malformed or duplicate principal rows; the job refuses the reconstruction otherwise.
- Count directors/writers as distinct people, keeping their provenance in `title.crew`. Multiple jobs by one person never become multiple people.
- Optionally resolve exact film IMDb IDs in Wikidata and gather declared human cast/crew members. Roles checked: cast (`P161`), director (`P57`), screenwriter (`P58`), producer (`P162`), composer (`P86`), cinematographer (`P344`), editor (`P1040`), voice actor (`P725`), performer (`P175`) and other film crew (`P3092`).
- Do not combine people from multiple Wikidata film entities sharing an IMDb ID. Retain all raw claims for inspection, but accept supplementary candidates only for one unambiguous film entity and a person ID present in cleaned `name.basics`.

The IMDb source catalog must match the one used for selection. All table transformations, deduplication, schema handling and joins run in Spark. Python gathers API responses and writes small metadata files. Wikidata responses retain request URL, retrieval time and entity URIs.

## Outputs

| Output | Purpose |
| --- | --- |
| `summary.json` | Counts, diagnoses, queried properties and confirmation that selection/requests were not changed |
| `film_cases.csv` / `film_cases/` | One row per affected film: title, year, counts, diagnosis and remaining deficit |
| `candidate_people.csv` / `candidate_people/` | Known IMDb people with names, sources, roles and whether they were already selected |
| `original_valid_principals/` | Existing valid principal rows for these films |
| `orphaned_principals/` | Relevant quarantined credits, when any exist |
| `wikidata_claims.csv` / `wikidata_claims/` | All gathered credit claims, including missing person IDs and absent biography articles |
| `wikidata_evidence/` | Original HTTP responses and retrieval lineage |

CSV files use UTF-8 and comma separators, with JSON arrays inside quoted cells. For Excel, import them as UTF-8/comma-separated files. Parquet directories are the Spark-readable counterparts. Generated results are ignored by Git.

## Reading the findings

`source_principals_insufficient` means the available IMDb source itself has fewer than three distinct principals; it does not establish that the real film involved fewer than three people. `missing_person_parents` indicates that enough source IDs existed but some could not be linked to the person table. `selection_discrepancy` indicates that valid principal counts and the selected count need investigation.

`crew_can_reach_three` and `all_evidence_can_reach_three` count known people supported by supplemental credits. These are proposed associations, not an assertion that those people appear in `title.principals` or that their biographies have been collected. A known IMDb ID does not guarantee an English Wikipedia biography. Raw Wikidata claims without an existing IMDb person ID remain evidence requiring additional mapping work.

The audit does not add proposed people to the collection queue, change the top-ten ranking, replace films with lower-ranked titles or mark the required biographical coverage complete. Any subsequent supplement must retain its real source and role and be integrated as a separate, traceable update after the current collection's inputs are preserved.

The course report's methodological explanation remains the group's own writing. `PROJECT.md` and its AI declaration are unchanged.

Property definitions are available in [Wikidata's film-property documentation](https://www.wikidata.org/wiki/Wikidata:WikiProject_Movies/Properties).
