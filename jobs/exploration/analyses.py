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
    # rank : position avec ex aequo (information) ; is_tie : égalité de votes
    w = Window.partitionBy("startYear").orderBy(F.desc("total_votes"))
    wt = Window.partitionBy("startYear", "total_votes")
    # top_rank : classement déterministe partagé par Q2 et Q3
    # (votes décroissants, puis genre alphabétique) -> exactement top_n genres
    w_top = Window.partitionBy("startYear").orderBy(F.desc("total_votes"), F.asc("genre"))
    return (agg.withColumn("rank", F.rank().over(w))
               .withColumn("is_tie", F.count("*").over(wt) > 1)
               .withColumn("top_rank", F.row_number().over(w_top)))


def compute_q2(stats, p):
    return (stats.filter(F.col("top_rank") <= p.top_n)
                 .select("startYear", "top_rank", "genre", "total_votes",
                         "n_movies", "avg_rating", "rank", "is_tie")
                 .orderBy("startYear", "top_rank"))


def compute_q3(stats, p):
    s = stats.withColumn("group", F.when(F.col("top_rank") <= p.top_n, "top")
                         .otherwise("rest"))
    year = Window.partitionBy("startYear")
    g = s.groupBy("startYear", "group").agg(
        F.count("*").alias("n_genres"),
        F.sum("total_votes").alias("votes"),
        F.avg("avg_rating").alias("mean_genre_rating"),
        F.avg("n_movies").alias("mean_movies_per_genre"))
    # try_divide : renvoie NULL si le total de l'année vaut 0 (au lieu de DIVIDE_BY_ZERO)
    return g.withColumn("vote_share",
                        F.try_divide(F.col("votes") * F.lit(100.0),
                                     F.sum("votes").over(year)))