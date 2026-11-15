from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import ArrayType

ACTING = ["actor", "actress"]


def _array_col(df, name):
    """Colonne `name` sous forme de tableau (déjà tableau, ou chaîne 'a,b,c')."""
    if isinstance(df.schema[name].dataType, ArrayType):
        return F.col(name)
    return F.when(
        F.col(name).isNull() | (F.col(name) == "\\N") | (F.col(name) == ""),
        F.array().cast("array<string>"),
    ).otherwise(F.split(F.col(name), ","))


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


def rated_movies(basics, ratings, min_votes=0) -> DataFrame:
    """Films (titleType='movie') avec note, une ligne par tconst."""
    movies = (basics.filter(F.col("titleType") == "movie")
              .select("tconst", "primaryTitle", "originalTitle", "startYear"))
    rated = (ratings.filter(F.col("averageRating").isNotNull()
                            & (F.col("numVotes") >= min_votes))
             .select("tconst", "averageRating", "numVotes"))
    return movies.join(rated, "tconst", "inner")


# --------------------------------------------------------------------- Q4
def one_hit_directors(basics, ratings, crew, names, high=8.0, low=6.0,
                      min_other=2, min_votes=0) -> DataFrame:
    """Directors with exactly one movie >= high and >= min_other other movies
    whose average rating is <= low."""
    movies = rated_movies(basics, ratings, min_votes)
    directed = (
        crew.select("tconst",
                    F.explode(_array_col(crew, "directors")).alias("nconst"))
        .join(movies, "tconst")
    )
    is_high = F.col("averageRating") >= high
    stats = directed.groupBy("nconst").agg(
        F.sum(is_high.cast("int")).alias("n_high"),
        F.sum((~is_high).cast("int")).alias("n_other"),
        F.avg(F.when(~is_high, F.col("averageRating"))).alias("avg_other"),
        F.first(F.when(is_high, F.col("primaryTitle")), ignorenulls=True).alias("hit_title"),
        F.max("averageRating").alias("hit_rating"),
    )
    return (
        stats.filter((F.col("n_high") == 1)
                     & (F.col("n_other") >= min_other)
                     & (F.col("avg_other") <= low))
        .join(names.select("nconst", "primaryName"), "nconst", "left")
        .select("nconst", "primaryName", "hit_title", "hit_rating", "n_other",
                F.round("avg_other", 2).alias("avg_other_rating"))
        .orderBy("avg_other_rating", "nconst")
    )


# --------------------------------------------------------------------- Q5
def declining_series(basics, ratings, episodes, min_seasons=3,
                     min_episodes=10, min_votes=0) -> DataFrame:
    """Series with >= min_seasons eligible ordinary seasons and >= min_episodes
    episodes in each counted season; worst episode in the last ordinary season
    and best in the first two ordinary seasons.

    Policy: season 0 is IMDb's specials bucket, not an ordinary season. It is
    excluded (together with null season numbers) from eligibility counts,
    season ordering, the last/first-two comparisons, and the best/worst episode
    candidates. The last season is the highest positive season number present;
    the first two are the two lowest positive season numbers present (gaps in
    numbering are not filled), whether or not each reaches min_episodes.
    Ties: rating, then most votes, then tconst (deterministic).

    Note: a series with a single positive season has no second season
    (second_season is NULL) and is therefore excluded by the final comparison.
    With min_seasons >= 2 this case cannot occur.
    """
    # Ordinary seasons only: drops season 0 and NULL season numbers.
    eps = episodes.filter(F.col("seasonNumber") > 0)

    seasons = (
        eps.groupBy("parentTconst", "seasonNumber").agg(F.count("*").alias("n_eps"))
        .groupBy("parentTconst").agg(
            F.sum((F.col("n_eps") >= min_episodes).cast("int")).alias("n_full_seasons"),
            F.max("seasonNumber").alias("last_season"),
            F.sort_array(F.collect_set("seasonNumber")).alias("season_list"),
        )
        .filter(F.col("n_full_seasons") >= min_seasons)
        # NULL when only one ordinary season exists (see docstring).
        .withColumn("second_season", F.element_at("season_list", 2))
        .drop("season_list")
    )

    rated = (
        eps.join(ratings.filter(F.col("numVotes") >= min_votes), "tconst")
        .select("parentTconst", "tconst", "seasonNumber", "averageRating", "numVotes")
    )

    def pick(order, prefix):
        w = Window.partitionBy("parentTconst").orderBy(*order)
        return (rated.withColumn("_r", F.row_number().over(w)).filter("_r = 1")
                .select("parentTconst",
                        F.col("tconst").alias(f"{prefix}_episode"),
                        F.col("seasonNumber").alias(f"{prefix}_season"),
                        F.col("averageRating").alias(f"{prefix}_rating")))

    worst = pick([F.col("averageRating").asc(), F.col("numVotes").desc(), "tconst"], "worst")
    best = pick([F.col("averageRating").desc(), F.col("numVotes").desc(), "tconst"], "best")

    series = (basics.filter(F.col("titleType").isin("tvSeries", "tvMiniSeries"))
              .select(F.col("tconst").alias("parentTconst"), "primaryTitle"))

    return (
        seasons.join(worst, "parentTconst").join(best, "parentTconst")
        .filter((F.col("worst_season") == F.col("last_season"))
                & (F.col("best_season") <= F.col("second_season")))
        .join(series, "parentTconst")
        .orderBy(F.col("best_rating") - F.col("worst_rating"), "parentTconst",
                 ascending=[False, True])
    )


# --------------------------------------------------------------------- Q6
# Contract: languages come from Wikidata P364 (matched through the exact IMDb
# id, P345), one row per distinct (tconst, language) pair. There is NO fallback
# to title.akas: a film without a declared language has no row in this table
# and is tracked through the collection status table (see q6_coverage).

def film_languages(languages_table) -> DataFrame:
    """Normalize the Wikidata P364 table: one row per distinct (tconst, language).

    Input columns: `tconst` and `language` (Wikidata language QID); any other
    columns (label, provenance, ...) are ignored here. Several languages for
    one film are valid and are all kept. Duplicate pairs are collapsed;
    null/blank tconst or language rows are dropped.
    """
    return (languages_table
            .select(F.trim(F.col("tconst")).alias("tconst"),
                    F.trim(F.col("language")).alias("language"))
            .filter(F.col("tconst").isNotNull() & (F.col("tconst") != "")
                    & F.col("language").isNotNull() & (F.col("language") != ""))
            .distinct())


def multilingual_actors(basics, ratings, principals, languages_table, names,
                        min_rating=7.0, min_votes=1000, min_languages=2) -> DataFrame:
    """Actors/actresses with successful movies in >= min_languages distinct
    Wikidata (P364) languages. A film with several declared languages counts
    in each of them; film-language pairs are deduplicated first.
    n_successful_movies is a distinct count of films, not of film-language rows.
    """
    hits = (rated_movies(basics, ratings, min_votes)
            .filter(F.col("averageRating") >= min_rating).select("tconst"))
    cast = (principals.filter(F.col("category").isin(ACTING))
            .select("tconst", "nconst").distinct())
    return (
        cast.join(hits, "tconst")
        .join(film_languages(languages_table), "tconst")
        .groupBy("nconst").agg(
            F.countDistinct("language").alias("n_languages"),
            F.sort_array(F.collect_set("language")).alias("languages"),
            F.countDistinct("tconst").alias("n_successful_movies"))
        .filter(F.col("n_languages") >= min_languages)
        .join(names.select("nconst", "primaryName"), "nconst", "left")
        .orderBy(F.col("n_languages").desc(), "nconst")
    )


def q6_actors_3plus_languages(q6_actors: DataFrame) -> DataFrame:
    """Subset of the main Q6 actor output with at least three languages."""
    return q6_actors.filter(F.col("n_languages") >= 3)


def q6_eligible_films(basics, ratings, min_rating=7.0, min_votes=1000) -> DataFrame:
    """Films meeting the Q6 success thresholds (the collection perimeter)."""
    return (rated_movies(basics, ratings, min_votes)
            .filter(F.col("averageRating") >= min_rating)
            .select("tconst").distinct())


def q6_coverage(eligible_films, collection_status) -> DataFrame:
    """Coverage over Q6-eligible films: number of films per collection status.

    `collection_status` has one row per collected film with columns `tconst`
    and `status` (e.g. declared, no_p364, no_wikidata_match, ambiguous_match,
    error). Eligible films absent from it get status 'not_collected'; a
    non-zero 'not_collected' count means the collection is incomplete and Q6
    must not be reported as final.
    """
    status = (collection_status.select("tconst", "status")
              .groupBy("tconst").agg(F.first("status").alias("status")))
    return (eligible_films.select("tconst")
            .join(status, "tconst", "left")
            .withColumn("status", F.coalesce(F.col("status"), F.lit("not_collected")))
            .groupBy("status").agg(F.count("*").alias("n_films"))
            .orderBy("status"))


# --------------------------------------------------------------------- Q7
def early_peak_actors(basics, ratings, principals, names, min_years=20,
                      min_movies=15, min_votes=0) -> DataFrame:
    """Actors with a career >= min_years and >= min_movies rated movies whose
    best movie is in the first half of the career AND whose first-half
    average rating exceeds the second-half average."""
    movies = (rated_movies(basics, ratings, min_votes)
              .filter(F.col("startYear").isNotNull()))
    cast = (principals.filter(F.col("category").isin(ACTING))
            .select("tconst", "nconst").distinct()
            .join(movies, "tconst"))

    careers = (cast.groupBy("nconst").agg(
                   F.min("startYear").alias("first_year"),
                   F.max("startYear").alias("last_year"),
                   F.countDistinct("tconst").alias("n_movies"))
               .filter((F.col("last_year") - F.col("first_year") >= min_years)
                       & (F.col("n_movies") >= min_movies))
               .withColumn("mid_year",
                           F.col("first_year") + (F.col("last_year") - F.col("first_year")) / 2))

    c = cast.join(careers, "nconst")
    early = F.col("startYear") <= F.col("mid_year")
    halves = c.groupBy("nconst").agg(
        F.avg(F.when(early, F.col("averageRating"))).alias("avg_early"),
        F.avg(F.when(~early, F.col("averageRating"))).alias("avg_late"))

    w = Window.partitionBy("nconst").orderBy(
        F.col("averageRating").desc(), F.col("numVotes").desc(), "startYear", "tconst")
    peak = (c.withColumn("_r", F.row_number().over(w)).filter("_r = 1")
            .select("nconst", F.col("primaryTitle").alias("peak_title"),
                    F.col("startYear").alias("peak_year"),
                    F.col("averageRating").alias("peak_rating")))

    return (
        careers.join(peak, "nconst").join(halves, "nconst")
        .filter((F.col("peak_year") <= F.col("mid_year"))
                & (F.col("avg_early") > F.col("avg_late")))
        .join(names.select("nconst", "primaryName"), "nconst", "left")
        .select("nconst", "primaryName", "first_year", "last_year", "n_movies",
                "peak_title", "peak_year", "peak_rating",
                F.round("avg_early", 2).alias("avg_early"),
                F.round("avg_late", 2).alias("avg_late"))
        .orderBy((F.col("avg_early") - F.col("avg_late")).desc(), "nconst")
    )


# --------------------------------------------------------------------- Q8
def movie_roles(crew, principals) -> DataFrame:
    """(tconst, nconst, role) with role in {actor, director, writer}."""
    directors = crew.select("tconst", F.explode(_array_col(crew, "directors")).alias("nconst"),
                            F.lit("director").alias("role"))
    writers = crew.select("tconst", F.explode(_array_col(crew, "writers")).alias("nconst"),
                          F.lit("writer").alias("role"))
    princ = (principals.filter(F.col("category").isin(ACTING + ["director", "writer"]))
             .select("tconst", "nconst",
                     F.when(F.col("category").isin(ACTING), "actor")
                     .otherwise(F.col("category")).alias("role")))
    return directors.unionByName(writers).unionByName(princ).distinct()


def multi_talented(basics, ratings, crew, principals, names, min_votes=0):
    """Returns (per_person, per_combination).
    per_person: people with >= 2 role types over their career, comparing the
    average rating of movies where they held several roles vs one role."""
    movies = rated_movies(basics, ratings, min_votes).select("tconst", "averageRating")
    roles = movie_roles(crew, principals).join(movies, "tconst")

    per_movie = (roles.groupBy("nconst", "tconst", "averageRating")
                 .agg(F.sort_array(F.collect_set("role")).alias("roles"))
                 .withColumn("n_roles", F.size("roles")))

    talents = (roles.groupBy("nconst")
               .agg(F.countDistinct("role").alias("n_role_types"))
               .filter(F.col("n_role_types") >= 2))
    pm = per_movie.join(talents, "nconst")

    multi = F.col("n_roles") >= 2
    per_person = (
        pm.groupBy("nconst", "n_role_types").agg(
            F.sum(multi.cast("int")).alias("n_multi_role_movies"),
            F.sum((~multi).cast("int")).alias("n_single_role_movies"),
            F.avg(F.when(multi, F.col("averageRating"))).alias("avg_multi"),
            F.avg(F.when(~multi, F.col("averageRating"))).alias("avg_single"))
        .filter((F.col("n_multi_role_movies") >= 1) & (F.col("n_single_role_movies") >= 1))
        .withColumn("rating_delta", F.round(F.col("avg_multi") - F.col("avg_single"), 2))
        .withColumn("avg_multi", F.round("avg_multi", 2))
        .withColumn("avg_single", F.round("avg_single", 2))
        .join(names.select("nconst", "primaryName"), "nconst", "left")
        .orderBy(F.col("rating_delta").desc(), "nconst")
    )

    per_combination = (
        pm.withColumn("combination", F.concat_ws("+", "roles"))
        .groupBy("combination").agg(
            F.count("*").alias("n_person_movies"),
            F.countDistinct("nconst").alias("n_people"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"))
        .orderBy(F.col("n_person_movies").desc())
    )
    return per_person, per_combination