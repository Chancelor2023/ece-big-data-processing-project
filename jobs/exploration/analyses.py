from pyspark.sql import Window, functions as F


def feature_films(basics, p):
    return basics.filter((F.col("titleType") == "movie")
                         & F.col("runtimeMinutes").isNotNull()
                         & (F.col("runtimeMinutes") >= p.min_runtime))


def compute_q1(basics, p):
    films = (feature_films(basics, p)
             .filter(F.col("genres").isNotNull())
             .select("tconst", "primaryTitle", "startYear", "runtimeMinutes",
                     F.explode("genres").alias("genre")))
    w = Window.partitionBy("genre")
    s = (films.withColumn("genre_avg", F.avg("runtimeMinutes").over(w))
         .withColumn("genre_std", F.stddev_samp("runtimeMinutes").over(w))
         .withColumn("genre_n", F.count("*").over(w))
         .filter((F.col("genre_n") >= p.min_genre_size) & (F.col("genre_std") > 0))
         .withColumn("z", (F.col("runtimeMinutes") - F.col("genre_avg")) / F.col("genre_std")))
    return (s.filter(F.abs("z") >= p.z_threshold)
            .withColumn("direction", F.when(F.col("z") > 0, "longer").otherwise("shorter")))


def genre_year_stats(basics, ratings, p):
    m = (feature_films(basics, p)
         .join(ratings, "tconst", "inner")   # 1-1 per contract
         .filter(F.col("averageRating").isNotNull()
                 & F.col("numVotes").isNotNull()
                 & (F.col("numVotes") >= p.min_votes)
                 & F.col("startYear").between(p.year_from, p.year_to)
                 & F.col("genres").isNotNull())
         .select("tconst", "startYear", "averageRating", "numVotes",
                 F.explode("genres").alias("genre")))
    agg = m.groupBy("startYear", "genre").agg(
        F.sum("numVotes").alias("total_votes"),
        F.countDistinct("tconst").alias("n_movies"),
        F.avg("averageRating").alias("avg_rating"))
    w = Window.partitionBy("startYear").orderBy(F.desc("total_votes"))
    wt = Window.partitionBy("startYear", "total_votes")
    return (agg.withColumn("rank", F.rank().over(w))
            .withColumn("is_tie", F.count("*").over(wt) > 1))


def compute_q2(stats, p):
    """Select top_n per year by votes desc, rating desc, then genre asc."""
    w = Window.partitionBy("startYear").orderBy(
        F.desc("total_votes"), F.desc("avg_rating"), F.asc("genre"))
    return (stats.withColumn("_q2_row", F.row_number().over(w))
            .filter(F.col("_q2_row") <= p.top_n)
            .drop("_q2_row"))


def compute_q3(stats, p):
    s = stats.withColumn("group", F.when(F.col("rank") <= p.top_n, "top")
                         .otherwise("rest"))
    year = Window.partitionBy("startYear")
    g = s.groupBy("startYear", "group").agg(
        F.count("*").alias("n_genres"),
        F.sum("total_votes").alias("votes"),
        F.avg("avg_rating").alias("mean_genre_rating"),
        F.avg("n_movies").alias("mean_movies_per_genre"))
    # Report percentages; a zero yearly total has no defined share and stays null.
    return g.withColumn(
        "vote_share",
        F.try_divide(F.col("votes") * 100.0, F.sum("votes").over(year)))