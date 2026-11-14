"""Official source headers and natural keys; no Spark dependency."""

TITLE_COLUMNS = {
    "title.basics": [
        "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
        "startYear", "endYear", "runtimeMinutes", "genres",
    ],
    "title.ratings": ["tconst", "averageRating", "numVotes"],
}
OTHER_COLUMNS = {
    "name.basics": [
        "nconst", "primaryName", "birthYear", "deathYear",
        "primaryProfession", "knownForTitles",
    ],
    "title.crew": ["tconst", "directors", "writers"],
    "title.episode": ["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
    "title.principals": [
        "tconst", "ordering", "nconst", "category", "job", "characters",
    ],
    "title.akas": [
        "titleId", "ordering", "title", "region", "language", "types",
        "attributes", "isOriginalTitle",
    ],
}
ALL_COLUMNS = {**TITLE_COLUMNS, **OTHER_COLUMNS}
TABLE_KEYS = {
    "title.basics": ["tconst"], "title.ratings": ["tconst"],
    "name.basics": ["nconst"], "title.crew": ["tconst"],
    "title.episode": ["tconst"],
    "title.principals": ["tconst", "ordering"],
    "title.akas": ["titleId", "ordering"],
}
