# IMDb Big Data Processing report

<!-- Final integration draft: review the report, code and evidence together before submission. -->

## Data preparation

### Results

#### Dataset scope and row counts

| Recorded item | Value |
| --- | --- |
| Source catalog | `20261114T115800Z-6c04bdd1` |
| Source download date | 14 November 2026 |
| Tables | 7 |
| Source columns | 39 |
| Column profiles | 78: 39 raw and 39 clean |

| Table | Source rows | Clean rows | Rows removed |
| --- | ---: | ---: | ---: |
| `title.basics` | 12,963,111 | 12,963,111 | 0 |
| `title.ratings` | 1,732,169 | 1,732,169 | 0 |
| `name.basics` | 15,816,005 | 15,816,005 | 0 |
| `title.crew` | 12,963,111 | 12,963,111 | 0 |
| `title.episode` | 10,034,862 | 10,034,857 | 5 |
| `title.principals` | 102,756,358 | 102,749,096 | 7,262 |
| `title.akas` | 60,424,702 | 60,424,694 | 8 |

Evidence: [table_counts.csv](report-assets/evidence-20261115/01-quality/table_counts.csv) and [IMDb source URLs, dates and SHA-256 fingerprints](report-assets/evidence-20261115/01-quality/imdb_source_fingerprints.csv).

#### Missing-value profiles for all columns

| Table | Column | Raw missing count | Raw missing % | Clean missing count | Clean missing % |
| --- | --- | ---: | ---: | ---: | ---: |
| `title.basics` | `tconst` | 0 | 0.000000% | 0 | 0.000000% |
| `title.basics` | `titleType` | 0 | 0.000000% | 0 | 0.000000% |
| `title.basics` | `primaryTitle` | 0 | 0.000000% | 0 | 0.000000% |
| `title.basics` | `originalTitle` | 0 | 0.000000% | 0 | 0.000000% |
| `title.basics` | `isAdult` | 0 | 0.000000% | 0 | 0.000000% |
| `title.basics` | `startYear` | 1,487,320 | 11.473480% | 1,487,320 | 11.473480% |
| `title.basics` | `endYear` | 12,798,642 | 98.731254% | 12,798,642 | 98.731254% |
| `title.basics` | `runtimeMinutes` | 8,050,574 | 62.103719% | 8,050,578 | 62.103750% |
| `title.basics` | `genres` | 548,144 | 4.228491% | 548,144 | 4.228491% |
| `title.ratings` | `tconst` | 0 | 0.000000% | 0 | 0.000000% |
| `title.ratings` | `averageRating` | 0 | 0.000000% | 0 | 0.000000% |
| `title.ratings` | `numVotes` | 0 | 0.000000% | 0 | 0.000000% |
| `name.basics` | `nconst` | 0 | 0.000000% | 0 | 0.000000% |
| `name.basics` | `primaryName` | 293 | 0.001853% | 293 | 0.001853% |
| `name.basics` | `birthYear` | 15,130,128 | 95.663399% | 15,130,128 | 95.663399% |
| `name.basics` | `deathYear` | 15,550,475 | 98.321131% | 15,550,503 | 98.321308% |
| `name.basics` | `primaryProfession` | 3,220,639 | 20.363164% | 3,220,639 | 20.363164% |
| `name.basics` | `knownForTitles` | 1,917,578 | 12.124288% | 1,917,596 | 12.124402% |
| `title.crew` | `tconst` | 0 | 0.000000% | 0 | 0.000000% |
| `title.crew` | `directors` | 5,793,144 | 44.689458% | 5,793,451 | 44.691826% |
| `title.crew` | `writers` | 6,434,239 | 49.634991% | 6,434,502 | 49.637020% |
| `title.episode` | `tconst` | 0 | 0.000000% | 0 | 0.000000% |
| `title.episode` | `parentTconst` | 0 | 0.000000% | 0 | 0.000000% |
| `title.episode` | `seasonNumber` | 2,126,237 | 21.188503% | 2,126,232 | 21.188463% |
| `title.episode` | `episodeNumber` | 2,126,237 | 21.188503% | 2,126,232 | 21.188463% |
| `title.principals` | `tconst` | 0 | 0.000000% | 0 | 0.000000% |
| `title.principals` | `ordering` | 0 | 0.000000% | 0 | 0.000000% |
| `title.principals` | `nconst` | 0 | 0.000000% | 0 | 0.000000% |
| `title.principals` | `category` | 0 | 0.000000% | 0 | 0.000000% |
| `title.principals` | `job` | 82,882,023 | 80.658778% | 82,877,164 | 80.659750% |
| `title.principals` | `characters` | 52,691,057 | 51.277661% | 52,687,706 | 51.278024% |
| `title.akas` | `titleId` | 0 | 0.000000% | 0 | 0.000000% |
| `title.akas` | `ordering` | 0 | 0.000000% | 0 | 0.000000% |
| `title.akas` | `title` | 0 | 0.000000% | 0 | 0.000000% |
| `title.akas` | `region` | 13,002,884 | 21.519153% | 13,002,882 | 21.519152% |
| `title.akas` | `language` | 19,665,115 | 32.544827% | 19,665,112 | 32.544827% |
| `title.akas` | `types` | 40,393,110 | 66.848671% | 40,393,106 | 66.848673% |
| `title.akas` | `attributes` | 60,107,437 | 99.474942% | 60,107,432 | 99.474946% |
| `title.akas` | `isOriginalTitle` | 0 | 0.000000% | 0 | 0.000000% |

Evidence: [Complete raw/clean profiles, including row-count denominators](report-assets/evidence-20261115/01-quality/column_profile.csv).

#### Five highest raw missing-value percentages

| Table | Column | Raw missing count | Raw missing % |
| --- | --- | ---: | ---: |
| `title.akas` | `attributes` | 60,107,437 | 99.474942% |
| `title.basics` | `endYear` | 12,798,642 | 98.731254% |
| `name.basics` | `deathYear` | 15,550,475 | 98.321131% |
| `name.basics` | `birthYear` | 15,130,128 | 95.663399% |
| `title.principals` | `job` | 82,882,023 | 80.658778% |

Evidence: [top_five_missing_raw.csv](report-assets/evidence-20261115/01-quality/top_five_missing_raw.csv).

#### Recorded corrections and quarantines

| Table | Field or issue | Count | Unit | Recorded action |
| --- | --- | ---: | --- | --- |
| `title.basics` | `invalid_runtimeMinutes` | 4 | Field occurrences | Set runtimeMinutes to NULL; retain the title row |
| `name.basics` | `deathYear_before_birthYear` | 28 | Field occurrences | Set deathYear to NULL; retain the person row |
| `name.basics` | `knownForTitles` | 68 | Reference occurrences | Remove absent title references from the array |
| `title.crew` | `directors` | 430 | Reference occurrences | Remove absent person references from the array |
| `title.crew` | `writers` | 1,159 | Reference occurrences | Remove absent person references from the array |
| `title.episode` | `parentTconst` | 5 | Rows for this reference check | Quarantine rows referencing an absent parent title |
| `title.principals` | `nconst` | 7,262 | Rows for this reference check | Quarantine rows referencing an absent person |
| `title.akas` | `titleId` | 8 | Rows for this reference check | Quarantine rows referencing an absent title |

Evidence: [corrections_and_quarantines.csv](report-assets/evidence-20261115/01-quality/corrections_and_quarantines.csv) and [cleaned-table data contract](docs/DATA_CONTRACT.md).

#### Recorded validation results

| Check | Recorded result |
| --- | --- |
| Global quality report | `passed` |
| Full seven-table dataset profiled | `true` |
| Parquet read-back validation | Passed for all seven tables |
| Duplicate rows quarantined | 0 in each table |
| Rejected rows reported in table_counts.csv | 0 in each table |

Evidence: [Full quality report](report-assets/evidence-20261115/01-quality/quality_report.original.json) and [table_counts.csv](report-assets/evidence-20261115/01-quality/table_counts.csv).

#### Recorded field types and validity constraints

| Field | Cleaned Spark type | Recorded validity or conversion rule |
| --- | --- | --- |
| `startYear`, `endYear`, `birthYear`, `deathYear` | `integer` | Parsed years from 1 to 9999; invalid values become NULL |
| `runtimeMinutes` | `integer` | Strictly positive minutes; invalid or non-positive values become NULL |
| `averageRating` | `double` | Values from 1 to 10 inclusive; invalid values become NULL |
| `numVotes` | `long` | Non-negative counts; zero is retained; invalid values become NULL |
| `isAdult` | `boolean` | Source 0 becomes false and 1 becomes true; missing/invalid values become NULL |
| `genres` | `array<string>` | Trimmed, distinct, sorted tokens; no usable tokens becomes NULL |
| `characters` | `array<string>` | Explicit JSON [] is retained as an empty array; invalid JSON becomes NULL |

Evidence: [Data contract](docs/DATA_CONTRACT.md), [title transformations and profiling](src/imdb_pipeline/preparation.py) and [other-table transformations](src/imdb_pipeline/other_preparation.py).

### Methodological explanation

The following seven IMDb files were processed:

| Table | Content |
| --- | --- |
| title.basics | General information about titles |
| title.ratings | Average ratings and total votes |
| name.basics | Information about each person |
| title.crew | Directors and writers associated with each title |
| title.episode | Episode-series relationships and season and episode numbers |
| title.principals | Credits and roles |
| title.akas | Alternative titles, regions and languages |

In total, we have 39 columns and 78 profiles.
Selection restrictions on films do not apply to this data preparation step.

The role of Spark

Spark reads TSV data, calculates profiles of missing values, applies data types and cleaning rules, validates keys and references, and then writes and re-reads the results. Python handles the downloading process and manages small metadata files.

The role of Parquet

Parquet serves as the storage format for cleaned tables, rather than as a computational engine. It preserves column data types—including arrays—and allows subsequent processing steps to load tables directly, without needing to start from the raw TSV files. Its columnar organization also enables the reading of only those columns that are actually required.

The `data/clean/current.json` manifest specifies the paths, schemas, and record counts for the validated catalog.

For profiling, values considered missing are NULL values, the `\N` marker, empty strings or strings consisting only of spaces, and, for array-type columns, NULL values or empty arrays.

To calculate the percentage, we divide `missing_count` by `row_count` and multiply the result by 100, using the table’s record count at the relevant stage (raw or cleaned).

Years and durations are treated as integers, ratings as doubles, vote counts as long integers, `isAdult` as a boolean, and `genres` as an array of strings.

A vote count of zero is valid and is not automatically treated as missing.

#### Corrections and validation

Cleaning distinguishes field corrections, removal of references within arrays, and row quarantine. An invalid value does not automatically cause the entire record to be deleted. Invalid runtimes become `NULL` while the other title information is retained. When a recorded death year precedes a known birth year, the death year is invalidated without removing the person. This removes the observed inconsistency without claiming to recover the true date or to prove that the birth year is correct.

References are checked against the cleaned parent tables. Missing title or person references are removed from the `knownForTitles`, `directors` and `writers` arrays. Episode, credit and alternative-title rows with missing mandatory parents are quarantined. Quarantined records remain available for diagnosis but are excluded from the published analytical tables. Corrected fields, removed reference occurrences and excluded rows have different units and are not added together as a number of deleted films.

Retaining unknown values as `NULL` avoids assigning arbitrary numerical information. Profiling also counts empty arrays as missing, but that measurement does not mean every empty array is invalid: an explicit `characters` value of `[]` remains an empty array in the cleaned data.

Before publication, the preparation jobs check recorded row counts, expected keys, references and Parquet read-back. Before/after profiles make the changes observable. An increase in missing values can result from invalidating previously populated but incorrect fields and does not automatically indicate poorer processing. Source URLs, dates and hashes identify the input snapshot, since IMDb downloads can change over time. Processing remains local inside a container; it is not a benchmark of a multi-machine Spark cluster.

Evidence: [data contract](docs/DATA_CONTRACT.md), [before/after profiles](report-assets/evidence-20261115/01-quality/column_profile.csv), [corrections and quarantines](report-assets/evidence-20261115/01-quality/corrections_and_quarantines.csv), and [quality report](report-assets/evidence-20261115/01-quality/quality_report.original.json).

### Alternatives and trade-offs

Deleting all incomplete rows would have simplified certain operations, but it would also have removed numerous records containing useful information. Conversely, automatically imputing missing years, durations, or ratings would have generated estimated values—potentially skewing averages and rankings. The chosen approach therefore retains usable data while explicitly representing unknown or invalid values.

Regarding missing references, keeping the identifiers as-is would have preserved the original sources within the analytical entries, but it would have resulted in "orphaned" joins (links without a parent). By quarantining and removing invalid references from the tables, this issue is made visible. However, these decisions may reduce the number of available attributions or associations; the absence of a parent in this catalog does not prove that the information does not exist in reality.

Processing via the `pandas` library was an alternative for certain stages, but the size of the datasets and the project’s requirement to use Spark favored a unified Spark-based transformation pipeline. While the CSV format would be more directly human-readable, it is less effective than Parquet at preserving complex data types. On the other hand, using Parquet introduces a dependency on a compatible reader tool. These comparisons reflect technical trade-offs; they are not based on a formal benchmark conducted as part of this project.

## Data exploration

### Results

#### Exploration results snapshot

| Recorded item | Value |
| --- | --- |
| Delivery status | Final integration candidate; peer review required before merge into main |
| Source catalog | `20261114T115800Z-6c04bdd1` |
| Exploration run | `20261115T174842Z-15e20a` |
| Run status / Q6 mode | `passed` / `final` |
| Analysis origin | Brett273, commit 3ad08a7; independent runner and P364 integration |

Evidence: [run.original.json](report-assets/independent-final-20261115/exploration/run.original.json).

| Question | Output | Rows | Complete result table |
| --- | --- | ---: | --- |
| Q1 | Runtime outliers | 13,700 | [q1_runtime_outliers.csv](report-assets/independent-final-20261115/exploration/q1_runtime_outliers.csv) |
| Q2 | Top three genres by year | 138 | [q2_top_genres.csv](report-assets/independent-final-20261115/exploration/q2_top_genres.csv) |
| Q3 | Top genres versus remaining genres | 92 | [q3_top_vs_rest.csv](report-assets/independent-final-20261115/exploration/q3_top_vs_rest.csv) |
| Q4 | One-hit directors | 181 | [q4_one_hit_directors.csv](report-assets/independent-final-20261115/exploration/q4_one_hit_directors.csv) |
| Q5 | Series matching the season criteria | 475 | [q5_declining_series.csv](report-assets/independent-final-20261115/exploration/q5_declining_series.csv) |
| Q6 | Actors/actresses with at least two languages | 11,815 | [q6_multilingual_actors.csv](report-assets/independent-final-20261115/exploration/q6_multilingual_actors.csv) |
| Q6 | Actors/actresses with at least three languages | 4,478 | [q6_actors_3plus_languages.csv](report-assets/independent-final-20261115/exploration/q6_actors_3plus_languages.csv) |
| Q7 | Actors/actresses matching the early-peak criteria | 5,600 | [q7_early_peak_actors.csv](report-assets/independent-final-20261115/exploration/q7_early_peak_actors.csv) |
| Q8 | Multi-talented people | 26,715 | [q8_multi_talented_people.csv](report-assets/independent-final-20261115/exploration/q8_multi_talented_people.csv) |
| Q8 | Role combinations | 7 | [q8_multi_talented_combinations.csv](report-assets/independent-final-20261115/exploration/q8_multi_talented_combinations.csv) |

#### Recorded execution parameters

| Parameter | Value |
| --- | --- |
| `min_runtime` | `40` |
| `z_threshold` | `2.0` |
| `min_genre_size` | `30` |
| `min_votes` | `1000` |
| `year_from` | `1980` |
| `year_to` | `2025` |
| `top_n` | `3` |
| `Q4_HIGH_RATING` | `8.0` |
| `Q4_LOW_AVG_RATING` | `6.0` |
| `Q4_MIN_OTHER_MOVIES` | `2` |
| `Q4_MIN_VOTES` | `100` |
| `Q5_MIN_EPISODES_PER_SEASON` | `10` |
| `Q5_MIN_EPISODE_VOTES` | `10` |
| `Q5_MIN_FULL_SEASONS` | `3` |
| `Q6_MIN_LANGUAGES` | `2` |
| `Q6_MIN_LANGUAGES_STRICT` | `3` |
| `Q6_SUCCESS_MIN_VOTES` | `1000` |
| `Q6_SUCCESS_RATING` | `7.0` |
| `Q7_MIN_CAREER_YEARS` | `20` |
| `Q7_MIN_MOVIES` | `15` |
| `Q7_MIN_VOTES` | `100` |
| `Q8_MIN_VOTES` | `100` |

Evidence: [run.original.json](report-assets/independent-final-20261115/exploration/run.original.json).

#### Q6 language-source coverage

| Recorded item | Value |
| --- | --- |
| Language run | `20261115T172917Z-0ad8a6` |
| Language snapshot | `20261115T174630Z-b9b01c` |
| Eligible films | 14,190 |
| Films with declared languages | 12,862 |
| Declared-language coverage | 90.64% |
| Distinct film–language pairs | 14,814 |
| Language / identity properties | `P364` / `P345` |
| Collection complete | `true` |

| Source status | Eligible films |
| --- | ---: |
| `declared` | 12,862 |
| `ambiguous_match` | 11 |
| `no_p364` | 812 |
| `no_wikidata_match` | 505 |

Evidence: [coverage.original.json](report-assets/independent-final-20261115/language_sources/coverage.original.json) and [Film–language lineage](report-assets/independent-final-20261115/language_sources/film_language_lineage.csv).

#### Q8 role-combination table

| Combination | Person–movie observations | Distinct people within combination | Average rating |
| --- | ---: | ---: | ---: |
| `actor+writer` | 11,073 | 8,232 | 5.50 |
| `actor+director+writer` | 9,189 | 6,184 | 5.51 |
| `actor+director` | 2,490 | 1,903 | 5.64 |
| `actor` | 132,040 | 17,412 | 5.83 |
| `director+writer` | 85,074 | 42,020 | 5.90 |
| `writer` | 47,437 | 16,160 | 5.93 |
| `director` | 41,348 | 13,956 | 5.94 |

Evidence: [q8_multi_talented_combinations.csv](report-assets/independent-final-20261115/exploration/q8_multi_talented_combinations.csv).

#### Recorded output validation

| Check | Result |
| --- | --- |
| Packaged output checks | 28 passed |
| Full Q1–Q8 execution | `true` |

Evidence: [VALIDATION.json](report-assets/independent-final-20261115/VALIDATION.json).

### Methodological explanation

The analyses utilize cleaned tables and parameters that are saved alongside the outputs. They do not all rely on the same subset of films: the restrictions regarding year, duration, and number of votes are specific to each individual question. Consequently, the results describe the populations actually selected, rather than the entirety of cinematic activity.

#### Q1 — Runtime outliers by genre

Titles classified as `movie`, with a known runtime of at least 40 minutes, are associated with each of their genres. For genres with at least 30 observations and a nonzero sample standard deviation, the analysis computes the mean runtime, sample standard deviation and standardized score `z = (runtime − genre mean) / genre standard deviation`. Observations with an absolute score of at least two are retained and labelled as shorter or longer.

This gives a reproducible definition of a notable difference while accounting for variation within each genre. It does not establish that an unusual runtime is erroneous. A multigenre film may appear more than once: the 13,700 output rows are film–genre observations, not necessarily distinct films. Q1 does not apply the 1980–2025 restriction used in Q2 and Q3.

#### Q2 and Q3 — Genre popularity and concentration

Q2 considers movies lasting at least 40 minutes, released from 1980 to 2025, with a known genre and rating and at least 1,000 votes. Genre popularity is represented by the sum of votes for its films within each year. This is an indicator of engagement on IMDb, rather than average quality or cinema admissions. Genres are ranked by descending votes and then alphabetically to break ties, producing exactly three genres per year. Vote ties remain visible in the output.

Q3 uses the same ranking to form the `top` and `rest` groups. A group's vote share uses the annual total of votes aggregated across genres as its denominator. The result is `NULL`, rather than zero, when that denominator is zero. The reported mean rating is a mean of genre-level means, not a weighted mean of all individual films.

Genres overlap: the votes of a multigenre film contribute to several genres. These shares therefore do not divide unique votes into mutually exclusive categories. In the local outputs, Drama appears in the top three in all 46 years and ranks first in 42. Annual top-three vote shares range from approximately 38.20% to 58.36%, with an arithmetic mean of the 46 annual shares of 44.74%. A genre's frequent first-place position does not imply that the top three always account for a majority of the votes counted across all genres.

#### Q4 — Directors with one highly rated film

Rated movies with at least 100 votes are linked to their directors. The result retains directors with exactly one film rated at least 8/10, at least two other films, and an average rating for those other films no higher than 6/10. The condition applies to their average; it does not require every other film to score at most six. The 181 results satisfy this definition within the observed, filtered filmography.

#### Q5 — Series meeting the season and episode criteria

Season zero and unknown season numbers are excluded from this analysis. A series needs at least three ordinary seasons with at least ten episodes each; additional seasons may be shorter. Episodes used to select the best and worst ratings must have at least ten votes. Ties are resolved by the number of votes and then the IMDb identifier.

The last season has the highest positive season number, even if it has fewer than ten episodes. The first two seasons are the two smallest positive season numbers actually present; missing numbers are not invented. The selected worst episode must be in the last season and the selected best episode in the first two. The 475 results satisfy this particular criterion, which is not a measurement of continuous decline or proof that every recent season is worse.

#### Q6 — Success across languages

A successful film is a `movie` rated at least 7/10 with at least 1,000 votes. Language collection covers all films meeting these criteria, rather than only the top-ten genre/year selection used for text enrichment. Languages come from Wikidata P364, matched through the exact IMDb identifier P345. Alternative-title languages in `title.akas` are not used as substitutes for film languages.

Film–language pairs are distinct. A multilingual film contributes all of its declared languages but is counted once in a person's number of successful films. Actor and actress credits are grouped to produce lists for at least two and at least three languages. A single multilingual film can therefore contribute several languages; the output does not by itself demonstrate independent careers in several language markets.

The outputs contain 11,815 people for at least two languages and 4,478 for at least three. The language source covers 12,862 of 14,190 eligible films, or 90.64%. Missing and ambiguous records remain identified and are not treated as either monolingual films or artistic failures.

#### Q7 — An early peak in observed film ratings

The observed career is reconstructed from actor and actress credits in rated movies with at least 100 votes and a known year. It must span at least twenty years between the first and last observed film and contain at least fifteen distinct films. The early/late boundary is the midpoint of that time interval, not the midpoint of the film count.

The highest-rated film must fall in the first half, and the average rating of that half must exceed the second-half average. Ties for the best film are resolved by votes, year and identifier. The 5,600 results represent an early peak according to the retained IMDb ratings. This does not directly measure revenue, fame or a complete real-world career, because excluded or unrecorded credits and films do not contribute.

#### Q8 — Multiple roles and film ratings

Actor, director and writer roles are reconstructed from crew and principal credits and deduplicated by person, film and role. Films require at least 100 votes. A person is considered multi-talented when at least two role types occur in the observed sample. The individual comparison additionally requires at least one film with multiple roles and one with a single role. The two groups' average ratings and their difference are then computed.

The individual output contains 26,715 people. The seven-combination table uses the broader population of people with multiple role types across their observed filmography; its counts must not be summed as unique people. Its averages describe person–film observations, and one film may contribute for several people. Differences between combinations do not establish a causal effect of holding multiple roles.

Evidence: [run manifest and parameters](report-assets/independent-final-20261115/exploration/run.original.json), [complete result index](report-assets/independent-final-20261115/INDEX.md), [language coverage](report-assets/independent-final-20261115/language_sources/coverage.original.json), and [analysis implementation](jobs/exploration/analyses.py).

### Alternatives and trade-offs

Popularity could instead be represented by the number of films, mean or median ratings, or external audience figures. Votes provide an available, reproducible indicator, but also reflect visibility, film age and IMDb's audience. Minimum-vote thresholds reduce the influence of ratings based on very few opinions while excluding some less visible productions.

For runtimes, quantiles or the interquartile range would be less sensitive to extreme observations than a standardized score. The selected rule is explicit, but runtimes are not assumed to follow a normal distribution, and the two-standard-deviation threshold is not a statistical significance test. The thresholds in the other questions are also operational definitions rather than universal boundaries of success or decline.

Rating averages could be weighted by votes, giving greater influence to the most visible films. Models controlling for genre, period or budget could examine some confounding factors, but would require additional data and assumptions. These results remain descriptive comparisons. Alternative implementations and a complete parameter-sensitivity study are not claimed to have been executed.


## Data enrichment

### Results and source coverage

#### Selection and published snapshot

| Recorded item | Value |
| --- | --- |
| Source catalog | `20261114T115800Z-6c04bdd1` |
| Enrichment run | `20261114T142005Z` |
| Final text snapshot | `20261115T105744Z-c082eb` |
| Recorded selection | `titleType=movie; 1980<=startYear<=2025; numVotes>0; known genre` |
| Recorded ranking | `numVotes DESC, averageRating DESC NULLS LAST, tconst ASC` |
| Maximum selected films per genre/year | 10 |
| Initial target principals per film | 3 |
| Film–genre–year selection rows | 10,277 |
| Unique selected films | 7,263 |
| Unique targeted people | 11,443 |
| Initial film–person associations | 21,695 |
| Films with fewer than three people in the initial selection | 53 |
| Collection status / processed requests | `completed` / 18,706 of 18,706 |

Evidence: [selection.original.json](report-assets/evidence-20261115/02-enrichment/selection.original.json) and [collection.original.json](report-assets/evidence-20261115/02-enrichment/collection.original.json).

#### Final text coverage

| Measure | Available | Denominator | Coverage |
| --- | ---: | ---: | ---: |
| Film synopses | 5,983 | 7,263 | 82.38% |
| Unique person biographies | 8,055 | 11,443 | 70.39% |
| Films with at least three available biographies | 5,825 | 7,263 | 80.20% |
| Films with a synopsis and at least three available biographies | 5,329 | 7,263 | 73.37% |

| Recorded validation | Result |
| --- | --- |
| Independent coverage verification | `passed` |
| Collection attempted for all targets | `true` |
| Full enrichment complete | `false` |

Evidence: [coverage.original.json](report-assets/evidence-20261115/02-enrichment/coverage.original.json) and [verification.original.json](report-assets/evidence-20261115/02-enrichment/verification.original.json).

#### Coverage across snapshots

| Snapshot | Synopses | Unique biographies | Films with at least three biographies | Films meeting both criteria |
| --- | ---: | ---: | ---: | ---: |
| `20261115T092501Z-6b1e0b` | 5,881 | 8,055 | 5,246 | 4,879 |
| `20261115T103509Z-ceaeb1` | 5,881 | 8,055 | 5,825 | 5,282 |
| `20261115T105744Z-c082eb` | 5,983 | 8,055 | 5,825 | 5,329 |

| Recorded addition | Count |
| --- | ---: |
| Supplementary associations to existing biographies | 916 |
| Films receiving supplementary associations | 791 |
| Additional films with three biographies after supplementation | 579 |
| Additional synopses recovered | 102 |

Evidence: [coverage_progression.csv](report-assets/evidence-20261115/02-enrichment/coverage_progression.csv), [coverage.original.json](report-assets/evidence-20261115/02-enrichment/coverage.original.json) and [synopsis_recovery_decisions.csv](report-assets/evidence-20261115/02-enrichment/synopsis_recovery_decisions.csv).

#### Final collection and text statuses

| Target kind | Status | Count | Target denominator |
| --- | --- | ---: | ---: |
| film | `no_english_article` | 332 | 7,263 |
| film | `available` | 5,983 | 7,263 |
| film | `missing_plot_section` | 378 | 7,263 |
| film | `ambiguous_match` | 6 | 7,263 |
| film | `not_found` | 564 | 7,263 |
| person | `no_english_article` | 1,200 | 11,443 |
| person | `available` | 8,055 | 11,443 |
| person | `missing_biography_intro` | 1 | 11,443 |
| person | `not_found` | 2,173 | 11,443 |
| person | `ambiguous_match` | 14 | 11,443 |

Evidence: [status_counts.csv](report-assets/evidence-20261115/02-enrichment/status_counts.csv).

| Available biographies per film | Films |
| ---: | ---: |
| 0 | 412 |
| 1 | 435 |
| 2 | 591 |
| 3 | 5,825 |

Evidence: [biographies_per_film.csv](report-assets/evidence-20261115/02-enrichment/biographies_per_film.csv).

| Remaining target list | Records | Evidence |
| --- | ---: | --- |
| Films without a synopsis | 1,280 | [films_without_synopsis.csv](report-assets/evidence-20261115/02-enrichment/films_without_synopsis.csv) |
| People without a biography | 3,388 | [people_without_biography.csv](report-assets/evidence-20261115/02-enrichment/people_without_biography.csv) |
| Films with fewer than three available biographies | 1,438 | [films_with_fewer_than_three_biographies.csv](report-assets/evidence-20261115/02-enrichment/films_with_fewer_than_three_biographies.csv) |

#### Published source metadata

| Metadata item | Recorded result |
| --- | --- |
| Available-text source records | 14,038: 5,983 films and 8,055 people |
| Per-record lineage fields | IMDb ID, Wikidata entity URI, article URL, source revision URL, revision ID, retrieval timestamp and license URL |

Evidence: [Per-record source revisions and licenses](report-assets/evidence-20261115/02-enrichment/available_text_sources.csv) and [Coverage by genre and year](report-assets/evidence-20261115/02-enrichment/genre_year_coverage.csv).

### Methodological explanation

Enrichment targets movies released from 1980 to 2025 with a known genre and positive vote count. Unlike Q1–Q3, this selection imposes no additional minimum runtime. Up to ten titles per genre and year are ranked by descending votes, descending rating and ascending identifier. Group memberships are retained, while film and person identifiers are deduplicated before collection. The 10,277 film–genre–year memberships therefore correspond to 7,263 unique films.

Up to three distinct people are initially selected from `title.principals` in credit order. A person holding several roles is not counted several times. The initial selection includes 53 films with fewer than three available people, so three biographies per film are not guaranteed even before external text availability is considered.

The text source is English Wikipedia. Matching uses IMDb identifiers in Wikidata and article sitelinks instead of relying only on similar titles or names. Collected texts are existing source content: no missing synopsis or biography is generated. Available records retain the entity, article, revision URL, retrieval timestamp and license information.

Selection, text cleaning, schemas, joins and coverage measurements run through Spark. The Python gathering component handles HTTP, caching and resumption. Requests are serial, spaced and retried a bounded number of times, with server `Retry-After` delays respected. A run can resume without requesting every saved article again. Completing the request list is distinguished from finding usable text for every target.

Two improvements reuse collected material. The first adds associations to existing available biographies only where IMDb credits confirm a film–person relationship. It preserves initial associations and adds 916 links without adding unique biography texts. The second recognizes additional synopsis headings in cached revisions. Uncertain identities, redirects to different articles and competing candidate sections are not automatically accepted. This recovers 102 additional synopses without rewriting already available texts.

The final publication contains 5,983 synopses out of 7,263 films (82.38%) and 8,055 unique biographies out of 11,443 people (70.39%). At film level, 5,825 have at least three available biographies, and 5,329 have both a synopsis and three biographies. Statuses distinguish missing pages, missing English articles, ambiguity and unavailable extractable content. Partial coverage is not presented as complete fulfilment of all enrichment targets.

Evidence: [selection](report-assets/evidence-20261115/02-enrichment/selection.original.json), [coverage progression](report-assets/evidence-20261115/02-enrichment/coverage_progression.csv), [final verification](report-assets/evidence-20261115/02-enrichment/verification.original.json), and [revision and license metadata](report-assets/evidence-20261115/02-enrichment/available_text_sources.csv).

### Alternatives and trade-offs

Using several sources or languages could improve coverage, but would also require reconciling additional identities, formats, revisions and licenses. A single source limits that complexity and makes missing cases easier to trace, at the cost of favouring films and people documented in English Wikipedia.

Approximate title matching could retrieve more candidates but introduce confusion between namesakes, remakes and film-series articles. Explicit identifiers and preserved ambiguity statuses favour traceability. Some cases can still be reviewed manually without automatically converting uncertain matches into available content.

Parallel requests might accelerate gathering, but would increase pressure on the service and complicate retries and recovery. Caching and text reuse reduce the required requests. Ranking by votes also favours visible titles; ranking by rating or another indicator would define a different sample. No measured benefit is claimed for an alternative strategy that was not executed.


## Streaming

### Results and live-test metrics

#### Versioned real-event validation session

| Recorded item | Value |
| --- | --- |
| Run ID | `live-validation-20261114-v2` |
| Session ID | `20261114T150245Z` |
| Start time (Europe/Paris) | 2026-11-14 16:03:08 |
| Source catalog | `20261114T115800Z-6c04bdd1` |
| Topic | `wikimedia.recentchange` |
| Synthetic events | `false` |
| IMDb title/person keys | 28,779,116 |
| Configured watermark | 5 minutes |

Evidence: [configuration.original.json](report-assets/evidence-20261115/03-streaming/configuration.original.json) and [session.original.json](report-assets/evidence-20261115/03-streaming/session.original.json).

#### Session results

| Measure | Recorded value |
| --- | --- |
| Elapsed time | 900.63 seconds |
| Committed batches | 29 |
| Processed rows | 1,776 |
| Matched events in the live test window | 130 |
| Total matched events across committed batches, including backlog | 203 |
| Observed rows dropped by watermark | 0 |
| Observed duplicate rows dropped | 0 |
| Final session status | `stopped` |
| Live test passed | `true` |

Evidence: [metrics.csv](report-assets/evidence-20261115/03-streaming/metrics.csv), [committed_batches.csv](report-assets/evidence-20261115/03-streaming/committed_batches.csv) and [microbatch_progress.csv](report-assets/evidence-20261115/03-streaming/microbatch_progress.csv).

| Detailed measurement | Versioned evidence |
| --- | --- |
| Per-batch event counts and matches | [committed_batches.csv](report-assets/evidence-20261115/03-streaming/committed_batches.csv) |
| Per-batch input rates, duration, watermark, offsets and observed drops | [microbatch_progress.csv](report-assets/evidence-20261115/03-streaming/microbatch_progress.csv) |
| Original Spark progress records | [progress.original.json](report-assets/evidence-20261115/03-streaming/progress.original.json) |
| Session summary | [session.original.json](report-assets/evidence-20261115/03-streaming/session.original.json) |

### Methodological explanation and late data

A producer reads Wikimedia changes and retains new and edited English Wikipedia articles in the main namespace, including bot edits. This broad scope supports observing several IMDb-related events during a short session. Events are published to Kafka, then read by Spark Structured Streaming, parsed with a schema and processed in micro-batches.

Relevance is established through an explicit chain: English article title, Wikidata sitelink, IMDb P345 identifier and presence among cleaned title or person keys. Matching is not based only on a keyword in the article title. Lookup responses are cached and their provenance is retained. One event may match several IMDb rows, so the metrics distinguish distinct events from matching rows.

Event time comes from the Wikipedia edit timestamp, whereas ingestion time represents arrival in Kafka. Invalid events are identified and retained in quarantine. A five-minute watermark is applied before deduplication by event identifier. It advances with observed event time and bounds retained state; it does not mean five minutes since reception. Events that are too old may be discarded. See the [Spark 4.0.1 documentation](https://spark.apache.org/docs/4.0.1/streaming/apis-on-dataframes-and-datasets.html#streaming-deduplication).

The delay trades tolerance for out-of-order arrival against retained state. It was not selected through an exhaustive optimization over several watermark values. Deduplication is bounded by this event-time policy and does not guarantee indefinite removal of every possible repetition.

The producer advances its resume cursor after Kafka acknowledgement. The consumer retains a checkpoint and writes each batch's outputs into an attempt directory before creating its commit marker. An already marked batch is consumed without repeating its published writes. This addresses possible re-execution of a `foreachBatch` batch: output guarantees depend on the sink implementation, as described in the [Spark documentation](https://spark.apache.org/docs/4.0.1/streaming/apis-on-dataframes-and-datasets.html#using-foreach-and-foreachbatch). The project does not claim a universal end-to-end exactly-once guarantee.

The versioned 14 November session lasted 900.63 seconds, with 29 committed batches, 1,776 processed rows and 130 relevant events in its live window. The 203 total matched events include backlog and must not be presented as 203 arrivals within that window. No naturally occurring late drops or duplicate drops were observed in this session. These zero observations do not establish every boundary case; separate synthetic tests cover deduplication, watermark handling and restart behavior.

Evidence: [configuration](report-assets/evidence-20261115/03-streaming/configuration.original.json), [metrics](report-assets/evidence-20261115/03-streaming/metrics.csv), [micro-batches](report-assets/evidence-20261115/03-streaming/microbatch_progress.csv), and [streaming tests](tests/test_streaming.py).

### Alternatives and trade-offs

Restricting the stream to enriched films would reduce lookup requests but could produce too few matches during fifteen minutes. The broad English-article filter increases observation opportunities while requiring more Wikidata lookups. Caching and grouped requests limit their cost.

A shorter watermark would retain less state but risk excluding more late data. A longer delay would tolerate greater lateness while requiring more state. Processing-time-only handling would simplify some aspects without representing the chronology of source edits as directly.

Availability and scalability remain limited. The pipeline depends on Wikimedia/Wikidata services, a fixed IMDb catalog and a local single-broker Kafka environment. External lookup requests can slow micro-batches. The test demonstrates operation during a particular window, not continuous production availability or measured distributed-cluster capacity.


## Open question

### Question and relevance

How do ratings of the first observed year of actor–director collaborations compare to later collaboration years?

This question goes beyond comparing averages between individuals by treating the actor–director pair as the unit of analysis. It examines the association between repeated collaboration and film reception without assuming improvement in advance. Answering it requires reconstructing collaborations through joins between titles, ratings, directors, acting credits and names. No additional external dataset is required.

### Data and results

#### Open-question snapshot and sample

| Recorded item | Value |
| --- | --- |
| Delivery status | Final integration candidate; peer review required before merge into main |
| Source catalog | `20261114T115800Z-6c04bdd1` |
| Run ID | `20261115T173528Z-ee767f` |
| Status | `passed` |
| Recorded unit | distinct actor–director pair; first year measured within the declared eligible sample |
| Within-year grouping | All movies in the first observed year remain in the first group |
| Associational results flag | `true` |
| Year range | 1980–2025 |
| Minimum votes | 1,000 |
| Eligible credited movies | 38,352 |
| All actor–director pairs in the eligible sample | 383,635 |
| All pair–movie observations in the eligible sample | 411,166 |
| Comparable pairs | 18,764 |
| Pair–movie observations for comparable pairs | 45,554 |

Evidence: [run.original.json](report-assets/independent-final-20261115/open_question/run.original.json) and [sample_coverage.csv](report-assets/independent-final-20261115/open_question/sample_coverage.csv).

#### Paired summary

| Measure | Value |
| --- | ---: |
| Mean pair-level first-year rating | 6.522594 |
| Mean pair-level later-year rating | 6.181816 |
| Mean paired difference (later minus first year) | -0.340778 |
| Median paired difference | -0.300000 |
| Pairs with a higher later-year mean | 5,828 |
| Pairs with a lower later-year mean | 12,050 |
| Pairs with equal means | 886 |

Evidence: [paired_summary.csv](report-assets/independent-final-20261115/open_question/paired_summary.csv). Summary decimals are displayed to six decimal places.

| Complete output | Rows | Evidence |
| --- | ---: | --- |
| `pair_comparison` | 18,764 | [pair_comparison.csv](report-assets/independent-final-20261115/open_question/pair_comparison.csv) |
| `pair_movie_observations` | 45,554 | [pair_movie_observations.csv](report-assets/independent-final-20261115/open_question/pair_movie_observations.csv) |
| `paired_summary` | 1 | [paired_summary.csv](report-assets/independent-final-20261115/open_question/paired_summary.csv) |
| `sample_coverage` | 1 | [sample_coverage.csv](report-assets/independent-final-20261115/open_question/sample_coverage.csv) |

#### Interpretation and limitations

The eligible sample contains 38,352 credited films, 383,635 distinct pairs and 411,166 pair–film observations. Of these pairs, 18,764 have films in their first observed year and in later years; their comparisons contain 45,554 observations.

The mean of pair-level first-year average ratings is 6.522594, compared with 6.181816 for later years. The mean individual difference, defined as later years minus the first year, is −0.340778 points; the approximate median difference is −0.300000. Later-year means are higher for 5,828 pairs, lower for 12,050 and equal for 886.

These results do not show a general improvement in ratings for repeated collaborations within the selected sample. They also do not establish that repeating a collaboration causes lower film quality. Genre, period, project characteristics and selection into repeated collaboration may contribute to the observed differences. The findings describe an association conditional on films and credits being present within the study scope.

### Methodological explanation

Movies are restricted to 1980–2025, with a known rating and at least 1,000 votes. Directors are extracted from `title.crew`, and actors/actresses from `title.principals`. After joining titles and ratings, each film–actor–director triple is deduplicated. Person names are added to make the results readable.

For each pair, the first observed year is the earliest year present in this eligible sample. Every film in that year belongs to the first group; films in subsequent years belong to the later group. This avoids inventing an order between releases in the same year. Only pairs with at least one film in each group are compared. The first observed year is not necessarily the pair's first actual career collaboration.

Ratings are averaged within each pair and period, and the individual differences are then aggregated. Each pair therefore receives equal weight in the mean difference, regardless of productivity. A film may contribute to several pairs, so observations are not all independent. The code does not exclude cases where the same person acts and directs; those cases are part of the computed scope.

Evidence: [parameters and manifest](report-assets/independent-final-20261115/open_question/run.original.json), [sample coverage](report-assets/independent-final-20261115/open_question/sample_coverage.csv), [paired summary](report-assets/independent-final-20261115/open_question/paired_summary.csv), [pair comparisons](report-assets/independent-final-20261115/open_question/pair_comparison.csv), and [join implementation](src/imdb_pipeline/collaborations.py).

### Alternatives and trade-offs

A film-level comparison would give greater weight to more productive pairs. Weighting by votes would emphasize the most visible films. Equal pair weights make the average pair-level difference straightforward to interpret, but a sparsely documented pair receives the same weight as one with many films.

Using years avoids imposing an unknown within-year release order, but does not distinguish the first film of a year from the next. The minimum-vote filter reduces the influence of sparsely rated titles while changing both the observed career and the population of eligible pairs.

Only pairs that collaborate again can enter the comparison; collaborations that are never repeated do not contribute a difference. This selection, incomplete principal credits and the time window can influence the result. Controlling for period, genre or other characteristics, or excluding pairs where the actor and director are the same person, would be possible extensions. None is presented as already implemented.
