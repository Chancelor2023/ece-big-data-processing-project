"""Factual actor–director collaboration comparisons on cleaned IMDb tables."""
from pyspark.sql import functions as F


def collaboration_frames(basics, ratings, crew, principals, names,
                         min_votes=1000, year_from=1980, year_to=2025):
    """Compare each pair's first observed year to later collaboration years.

    Dates within a year are unavailable: all films in the pair's first year
    are in the first-year group. This is an association, not a causal estimate.
    A movie may appear for several distinct actor/director pairs.
    """
    movies = (basics.filter((F.col('titleType') == 'movie') &
                           F.col('startYear').between(year_from, year_to))
              .select('tconst', 'primaryTitle', 'startYear')
              .join(ratings.select('tconst', 'averageRating', 'numVotes'), 'tconst')
              .filter(F.col('averageRating').isNotNull() & (F.col('numVotes') >= min_votes)))
    directors = crew.select('tconst', F.explode('directors').alias('director_id')).distinct()
    actors = (principals.filter(F.col('category').isin('actor', 'actress'))
              .select('tconst', F.col('nconst').alias('actor_id')).distinct())
    credits = (movies.join(directors, 'tconst').join(actors, 'tconst')
               .dropDuplicates(['tconst', 'actor_id', 'director_id']))
    first = credits.groupBy('actor_id', 'director_id').agg(
        F.min('startYear').alias('first_observed_year'),
        F.max('startYear').alias('last_observed_year'),
        F.countDistinct('tconst').alias('pair_movie_count'))
    observations = (credits.join(first, ['actor_id', 'director_id'])
                    .withColumn('period', F.when(F.col('startYear') == F.col('first_observed_year'),
                                               'first_observed_year').otherwise('later_years')))
    first_group = F.col('period') == 'first_observed_year'
    pairs = (observations.groupBy('actor_id', 'director_id', 'first_observed_year',
                                 'last_observed_year', 'pair_movie_count')
              .agg(F.sum(first_group.cast('int')).alias('first_year_movies'),
                   F.sum((~first_group).cast('int')).alias('later_year_movies'),
                   F.avg(F.when(first_group, F.col('averageRating'))).alias('first_year_mean_rating'),
                   F.avg(F.when(~first_group, F.col('averageRating'))).alias('later_year_mean_rating'))
              .filter((F.col('pair_movie_count') >= 2) & (F.col('later_year_movies') > 0))
              .withColumn('rating_difference', F.col('later_year_mean_rating') - F.col('first_year_mean_rating'))
              .join(names.select(F.col('nconst').alias('actor_id'), F.col('primaryName').alias('actor_name')),
                    'actor_id', 'left')
              .join(names.select(F.col('nconst').alias('director_id'), F.col('primaryName').alias('director_name')),
                    'director_id', 'left'))
    repeated = pairs.select('actor_id', 'director_id')
    pair_films = observations.join(repeated, ['actor_id', 'director_id'], 'inner')
    summary = pairs.agg(
        F.count('*').alias('pairs'),
        F.avg('first_year_mean_rating').alias('mean_pair_first_year_rating'),
        F.avg('later_year_mean_rating').alias('mean_pair_later_year_rating'),
        F.avg('rating_difference').alias('mean_paired_difference'),
        F.expr('percentile_approx(rating_difference, 0.5)').alias('median_paired_difference'),
        F.sum((F.col('rating_difference') > 0).cast('long')).alias('pairs_higher_later'),
        F.sum((F.col('rating_difference') < 0).cast('long')).alias('pairs_lower_later'),
        F.sum((F.col('rating_difference') == 0).cast('long')).alias('pairs_equal'))
    coverage = observations.agg(F.countDistinct('tconst').alias('eligible_credited_movies'),
        F.count('*').alias('all_pair_movie_observations'),
        F.countDistinct(F.struct('actor_id', 'director_id')).alias('all_actor_director_pairs'))
    return pairs, pair_films, summary, coverage
