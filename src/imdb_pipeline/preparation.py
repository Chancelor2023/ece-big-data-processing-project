"""Spark transformations for the first two IMDb tables."""

import gzip
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

TITLE_COLUMNS = {
    "title.basics": [
        "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
        "startYear", "endYear", "runtimeMinutes", "genres",
    ],
    "title.ratings": ["tconst", "averageRating", "numVotes"],
}


def read_tsv(spark: SparkSession, path: Path, table: str) -> DataFrame:
    """Read literal TSV fields, preserving quotes and rejecting wrong widths."""
    columns = TITLE_COLUMNS[table]
    header = "\t".join(columns)
    with gzip.open(path, "rt", encoding="utf-8") as source:
        if source.readline().rstrip("\r\n") != header:
            raise ValueError(f"Unexpected header for {table}: {path}")
    lines = spark.read.text(str(path)).filter(F.col("value") != header)
    fields = F.split(F.col("value"), "\t", -1)
    return lines.select(
        *[F.get(fields, i).alias(name) for i, name in enumerate(columns)],
        F.col("value").alias("_raw_line"),
        F.size(fields).alias("_field_count"),
    )


def normalized(column):
    value = F.trim(column)
    return F.when(value.isNull() | value.isin("", "\\N"), None).otherwise(value)


def prepare_rows(raw: DataFrame, table: str) -> DataFrame:
    """Type and validate fields; retain usable rows and audit corrections."""
    columns = TITLE_COLUMNS[table]
    source = raw.select(
        *[normalized(F.col(c)).alias(c) for c in columns],
        "_raw_line", "_field_count",
    )
    issues = []

    def integer(name, minimum, maximum=None, sql_type="int"):
        value = F.expr(f"try_cast(`{name}` as {sql_type})")
        valid = value.isNotNull() & (value >= minimum)
        if maximum is not None:
            valid = valid & (value <= maximum)
        issues.append(F.when(
            F.col(name).isNotNull() & ~valid,
            F.lit(f"invalid_{name}"),
        ))
        return F.when(valid, value)

    if table == "title.basics":
        adult = F.col("isAdult")
        issues.append(F.when(
            adult.isNotNull() & ~adult.isin("0", "1"),
            F.lit("invalid_isAdult"),
        ))
        start = integer("startYear", 1, 9999)
        end = integer("endYear", 1, 9999)
        reversed_years = start.isNotNull() & end.isNotNull() & (end < start)
        issues.append(F.when(reversed_years, F.lit("endYear_before_startYear")))
        duration = integer("runtimeMinutes", 1)
        tokens = F.transform(F.split(F.col("genres"), ","), lambda x: F.trim(x))
        tokens = F.filter(tokens, lambda x: x.isNotNull() & ~x.isin("", "\\N"))
        genres = F.array_sort(F.array_distinct(tokens))
        issues.append(F.when(
            F.col("genres").isNotNull() & (F.size(tokens) != F.size(genres)),
            F.lit("duplicate_genre_tokens"),
        ))
        issues.append(F.when(F.size(genres) > 3, F.lit("genres_more_than_three")))
        expressions = [
            F.col("tconst"), F.col("titleType"), F.col("primaryTitle"),
            F.col("originalTitle"),
            F.when(adult.isin("0", "1"), adult == "1").alias("isAdult"),
            start.alias("startYear"),
            F.when(~reversed_years, end).alias("endYear"),
            duration.alias("runtimeMinutes"),
            F.when(F.size(genres) > 0, genres)
            .otherwise(F.lit(None).cast("array<string>"))
            .alias("genres"),
        ]
    else:
        rating = F.expr("try_cast(averageRating as double)")
        valid_rating = (
            rating.isNotNull() & ~F.isnan(rating) & rating.between(1.0, 10.0)
        )
        issues.append(F.when(
            F.col("averageRating").isNotNull() & ~valid_rating,
            F.lit("invalid_averageRating"),
        ))
        votes = integer("numVotes", 0, sql_type="bigint")
        expressions = [
            F.col("tconst"),
            F.when(valid_rating, rating).alias("averageRating"),
            votes.alias("numVotes"),
        ]
    rejection = (
        F.when(F.col("_field_count") != len(columns), "invalid_field_count")
        .when(
            F.col("tconst").isNull() | ~F.col("tconst").rlike(r"^tt[0-9]+$"),
            "invalid_tconst",
        )
    )
    return source.select(
        *expressions, "_raw_line", "_field_count",
        F.array_compact(F.array(*issues)).alias("_issues"),
        rejection.alias("_reject_reason"),
    )


def rank_records(prepared: DataFrame, table: str, partitions: int) -> DataFrame:
    """Keep the most complete record per key, with deterministic tie breaking."""
    columns = TITLE_COLUMNS[table]
    completeness = sum(F.col(c).isNotNull().cast("int") for c in columns)
    canonical = F.to_json(F.struct(*[F.col(c) for c in columns]))
    window = Window.partitionBy("tconst").orderBy(
        completeness.desc(), canonical.asc(), F.col("_raw_line").asc()
    )
    return (
        prepared.filter(F.col("_reject_reason").isNull())
        .repartition(partitions, "tconst")
        .withColumn("_key_rank", F.row_number().over(window))
    )


def mark_rating_parents(ratings: DataFrame, basics: DataFrame) -> DataFrame:
    parents = basics.select("tconst").withColumn("_parent_exists", F.lit(True))
    return ratings.join(parents, "tconst", "left")


def profile_columns(frame: DataFrame, table: str, stage: str) -> tuple:
    """Collect only a single aggregate row, never the underlying title table."""
    columns = TITLE_COLUMNS[table]
    aggregates = [F.count("*").alias("_rows")]
    for i, name in enumerate(columns):
        column = F.col(name)
        if dict(frame.dtypes)[name].startswith("array"):
            missing = column.isNull() | (F.size(column) == 0)
        else:
            missing = (
                column.isNull()
                | (F.trim(column.cast("string")) == "")
                | (column.cast("string") == "\\N")
            )
        aggregates.append(F.count(F.when(missing, 1)).alias(f"m{i}"))
    result = frame.agg(*aggregates).first().asDict()
    count = result["_rows"]
    profile = [
        {
            "table": table, "stage": stage, "column": name,
            "row_count": count, "missing_count": result[f"m{i}"],
            "missing_percent": round(100 * result[f"m{i}"] / count, 6)
            if count else None,
        }
        for i, name in enumerate(columns)
    ]
    return count, profile
