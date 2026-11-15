"""Supplement film/person links using existing, sourced biographies only."""

from pyspark.sql import Window, functions as F


def biography_coverage(films, links, people):
    available = people.filter(F.col("status") == "available").select("nconst")
    counts = (links.join(available, "nconst", "inner").groupBy("tconst")
              .agg(F.countDistinct("nconst").alias("available_biographies")))
    return films.select("tconst").join(counts, "tconst", "left").fillna(
        0, ["available_biographies"]
    )


def cached_biography_supplements(principals, original_links, people, coverage,
                                 minimum=3):
    """Choose only missing links, bounded by each film's biography deficit.

    No HTTP, text generation, changed film ranking, or replacement of the
    original principals. An existing available biography and a cleaned IMDb
    principal credit are both required. Ordering ties use the person ID.
    """
    deficient = coverage.filter(F.col("available_biographies") < minimum)
    available = people.filter(F.col("status") == "available").select(
        "nconst", "primaryName",
        F.col("source_url").alias("biography_source_url"),
        F.col("revision_id").alias("biography_revision_id"),
    )
    candidates = (principals
        .join(F.broadcast(deficient.select("tconst")), "tconst", "left_semi")
        .join(available, "nconst", "inner")
        .join(original_links.select("tconst", "nconst"),
              ["tconst", "nconst"], "left_anti")
        .groupBy("tconst", "nconst", "primaryName", "biography_source_url",
                 "biography_revision_id")
        .agg(F.min("ordering").alias("first_ordering"),
             F.sort_array(F.collect_set("category")).alias("categories"))
        .join(deficient, "tconst", "inner"))
    ordering = Window.partitionBy("tconst").orderBy("first_ordering", "nconst")
    return (candidates.withColumn("supplement_rank", F.row_number().over(ordering))
            .filter(F.col("supplement_rank") <= minimum - F.col("available_biographies"))
            .drop("available_biographies"))
