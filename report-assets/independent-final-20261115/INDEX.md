# Independent final query outputs

Factual outputs only. Contributors write REPORT.md explanations themselves.

| Output | Rows | CSV |
| --- | --- | --- |
| q1_runtime_outliers | 13700 | [q1_runtime_outliers.csv](exploration/q1_runtime_outliers.csv) |
| q2_top_genres | 138 | [q2_top_genres.csv](exploration/q2_top_genres.csv) |
| q3_top_vs_rest | 92 | [q3_top_vs_rest.csv](exploration/q3_top_vs_rest.csv) |
| q4_one_hit_directors | 181 | [q4_one_hit_directors.csv](exploration/q4_one_hit_directors.csv) |
| q5_declining_series | 475 | [q5_declining_series.csv](exploration/q5_declining_series.csv) |
| q6_multilingual_actors | 11815 | [q6_multilingual_actors.csv](exploration/q6_multilingual_actors.csv) |
| q6_actors_3plus_languages | 4478 | [q6_actors_3plus_languages.csv](exploration/q6_actors_3plus_languages.csv) |
| q7_early_peak_actors | 5600 | [q7_early_peak_actors.csv](exploration/q7_early_peak_actors.csv) |
| q8_multi_talented_people | 26715 | [q8_multi_talented_people.csv](exploration/q8_multi_talented_people.csv) |
| q8_multi_talented_combinations | 7 | [q8_multi_talented_combinations.csv](exploration/q8_multi_talented_combinations.csv) |
| pair_comparison | 18764 | [pair_comparison.csv](open_question/pair_comparison.csv) |
| pair_movie_observations | 45554 | [pair_movie_observations.csv](open_question/pair_movie_observations.csv) |
| paired_summary | 1 | [paired_summary.csv](open_question/paired_summary.csv) |
| sample_coverage | 1 | [sample_coverage.csv](open_question/sample_coverage.csv) |

[Language coverage](language_sources/coverage.original.json)
[Language lineage](language_sources/film_language_lineage.csv)
[Read-back validation](VALIDATION.json)
[Source files](SOURCES.csv)

Analysis functions originate from Brett273 commit 3ad08a7. The independent runner,
P364 integration and actor–director question are documented in docs/INDEPENDENT_FINALIZATION.md.
The first collaboration year is observed within the declared eligible sample;
all movies in that year are grouped together. A movie can occur in multiple pairs.
Actor and director roles can belong to the same person; no distinct-person filter is imposed.
The paired tables report associations, not causal effects.
