"""Spark normalization of raw P345/P364 responses and final-publication guards."""
from pyspark.sql import functions as F

BATCH_SCHEMA = '''batch_id long, target_ids array<string>, response struct<
url:string,retrieved_at_utc:string,raw_response:string>'''
RESPONSE_SCHEMA = '''results struct<bindings:array<struct<
imdb:struct<value:string>,item:struct<value:string>,language:struct<value:string>>>>'''
ENTITY = r'^https?://www\.wikidata\.org/entity/Q[1-9][0-9]*$'


def prepare_languages(raw, eligible):
    """Retain every declared language of a single exact film identity.

    Different film entities are ambiguous; multiple languages of one entity
    are valid. All value normalization, joins and status assignment use Spark.
    """
    targets = raw.select('batch_id', F.explode('target_ids').alias('tconst'),
                         F.col('response.url').alias('source_query_url'),
                         F.col('response.retrieved_at_utc').alias('retrieved_at_utc'))
    parsed = raw.withColumn('parsed', F.from_json('response.raw_response', RESPONSE_SCHEMA))
    bindings = (parsed.select('batch_id', F.explode_outer('parsed.results.bindings').alias('b'))
                .select('batch_id', F.col('b.imdb.value').alias('tconst'),
                        F.col('b.item.value').alias('entity_uri'),
                        F.col('b.language.value').alias('language_uri'))
                .withColumn('valid_entity', F.col('entity_uri').rlike(ENTITY))
                .withColumn('valid_language', F.col('language_uri').rlike(ENTITY))
                .withColumn('invalid',
                    (F.col('entity_uri').isNotNull() & ~F.col('valid_entity')) |
                    (F.col('language_uri').isNotNull() &
                     (~F.col('valid_language') | F.col('entity_uri').isNull()))))
    joined = targets.join(bindings.withColumn('binding_present', F.lit(1)),
                          ['batch_id', 'tconst'], 'left')
    grouped = joined.groupBy('tconst', 'source_query_url', 'retrieved_at_utc').agg(
        F.sum(F.coalesce('binding_present', F.lit(0))).alias('binding_count'),
        F.countDistinct(F.when(F.col('valid_entity'), F.col('entity_uri'))).alias('entity_count'),
        F.countDistinct(F.when(F.col('valid_language'), F.col('language_uri'))).alias('language_count'),
        F.max(F.coalesce(F.col('invalid').cast('int'), F.lit(0))).alias('invalid'))
    grouped = grouped.withColumn('status',
        F.when((F.col('binding_count') == 0) | (F.col('invalid') > 0), 'invalid_response')
         .when(F.col('entity_count') > 1, 'ambiguous_match')
         .when(F.col('entity_count') == 0, 'no_wikidata_match')
         .when(F.col('language_count') == 0, 'no_p364').otherwise('declared'))
    statuses = (eligible.select('tconst').join(grouped, 'tconst', 'left')
                .withColumn('status', F.coalesce('status', F.lit('not_collected'))))
    accepted = statuses.filter(F.col('status') == 'declared').select('tconst')
    languages = (bindings.filter(F.col('valid_entity') & F.col('valid_language'))
                 .join(accepted, 'tconst', 'inner')
                 .join(targets.select('batch_id', 'tconst', 'source_query_url', 'retrieved_at_utc'),
                       ['batch_id', 'tconst'], 'inner')
                 .select('tconst', F.regexp_extract('language_uri', r'/(Q[1-9][0-9]*)$', 1).alias('language'),
                         'entity_uri', 'language_uri', 'source_query_url', 'retrieved_at_utc')
                 .dropDuplicates(['tconst', 'language'])
                 .withColumn('property_uri', F.lit('https://www.wikidata.org/wiki/Property:P364')))
    return languages, statuses


def require_final_language_coverage(coverage, source_catalog, min_rating, min_votes):
    """A completed, valid collection may contain documented unavailable films."""
    if coverage.get('source_catalog') != source_catalog:
        raise ValueError('Q6 language catalog differs from IMDb catalog')
    if coverage.get('min_rating') != min_rating or coverage.get('min_votes') != min_votes:
        raise ValueError('Q6 language collection thresholds differ from runner thresholds')
    if not coverage.get('collection_complete') or coverage.get('status') != 'passed':
        raise ValueError('Q6 requires a completed and validated P364 publication')
    counts = coverage.get('status_counts', {})
    allowed = {'declared', 'no_p364', 'no_wikidata_match', 'ambiguous_match'}
    if set(counts) - allowed or any(not isinstance(v, int) or v < 0 for v in counts.values()):
        raise ValueError('Q6 contains unsupported collection statuses or counts')
    if any(counts.get(k, 0) for k in ('not_collected', 'invalid_response', 'error', 'api_error')):
        raise ValueError('Q6 has uncollected or invalid responses')
    if sum(counts.values()) != coverage.get('eligible_films'):
        raise ValueError('Q6 coverage denominator is inconsistent')
    if coverage.get('declared_films', 0) <= 0 or counts.get('declared', 0) != coverage.get('declared_films'):
        raise ValueError('Q6 requires nonzero, consistent language coverage')
