"""Publish validated P364 film-language pairs and coverage with Spark."""
import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from uuid import uuid4
from pyspark import StorageLevel
from pyspark.sql import functions as F
from imdb_pipeline.config import PROJECT_ROOT, CLEAN_DIR
from imdb_pipeline.languages import BATCH_SCHEMA, prepare_languages, require_final_language_coverage
from imdb_pipeline.spark import create_spark
from jobs.preparation.prepare_titles import atomic_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id')
    args = parser.parse_args()
    base = PROJECT_ROOT / 'data/languages'
    run = args.run_id or json.loads((base / 'current_selection.json').read_text())['run_id']
    if not re.fullmatch(r'[A-Za-z0-9_-]+', run):
        parser.error('Invalid run ID')
    root = base / 'runs' / run
    selection = json.loads((root / 'selection.json').read_text())
    collection = json.loads((root / 'collection.json').read_text())
    if collection.get('status') != 'completed' or collection.get('processed_films') != selection['eligible_films']:
        raise ValueError('Complete P364 collection before final publication')
    catalog_bytes = (CLEAN_DIR / 'current.json').read_bytes()
    if hashlib.sha256(catalog_bytes).hexdigest() != selection['catalog_sha256']:
        raise ValueError('IMDb catalog differs from language selection')
    snapshot = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:6]
    output = root / 'clean' / snapshot
    spark = create_spark('q6-prepare-p364-languages')
    try:
        eligible = spark.read.parquet(str(root / 'targets'))
        raw = spark.read.schema(BATCH_SCHEMA).option('multiLine', True).json(str(root / 'raw' / '*.json'))
        if raw.count() != collection['expected_batches']:
            raise ValueError('Raw batch count differs from completed collection')
        requested = raw.select(F.explode('target_ids').alias('tconst'))
        if requested.groupBy('tconst').count().filter('count != 1').limit(1).count():
            raise ValueError('Duplicate requested films in raw batches')
        if requested.exceptAll(eligible.select('tconst')).limit(1).count() or eligible.select('tconst').exceptAll(requested).limit(1).count():
            raise ValueError('Raw requests do not cover the exact eligible selection')
        languages, statuses = prepare_languages(raw, eligible)
        languages = languages.persist(StorageLevel.DISK_ONLY)
        statuses = statuses.persist(StorageLevel.DISK_ONLY)
        counts = {r.status: r['count'] for r in statuses.groupBy('status').count().collect()}
        coverage = {'status': 'passed', 'run_id': run, 'snapshot': snapshot,
                    'source_catalog': selection['source_catalog'], 'min_rating': selection['min_rating'],
                    'min_votes': selection['min_votes'], 'eligible_films': selection['eligible_films'],
                    'declared_films': counts.get('declared', 0), 'status_counts': counts,
                    'collection_complete': True, 'film_language_pairs': languages.count(),
                    'coverage_percent': 100.0 * counts.get('declared', 0) / selection['eligible_films'],
                    'property': 'P364', 'identity_property': 'P345',
                    'prepared_at_utc': datetime.now(timezone.utc).isoformat()}
        require_final_language_coverage(coverage, selection['source_catalog'], selection['min_rating'], selection['min_votes'])
        if languages.filter('source_query_url IS NULL OR retrieved_at_utc IS NULL').limit(1).count():
            raise ValueError('Available languages lack provenance')
        if languages.groupBy('tconst', 'language').count().filter('count != 1').limit(1).count():
            raise ValueError('Duplicate film-language keys')
        languages.write.parquet(str(output / 'languages'))
        statuses.write.parquet(str(output / 'collection_status'))
        if spark.read.parquet(str(output / 'languages')).count() != coverage['film_language_pairs']:
            raise ValueError('Language read-back count differs')
        if spark.read.parquet(str(output / 'collection_status')).count() != selection['eligible_films']:
            raise ValueError('Status read-back count differs')
        if (CLEAN_DIR / 'current.json').read_bytes() != catalog_bytes:
            raise ValueError('IMDb catalog changed before publication')
        atomic_json(output / 'coverage.json', coverage)
        pointer = {'run_id': run, 'snapshot': snapshot, 'source_catalog': selection['source_catalog'],
                   'path': 'runs/' + run + '/clean/' + snapshot}
        atomic_json(base / 'current.json', pointer)
        atomic_json(PROJECT_ROOT / 'results/languages' / run / snapshot / 'coverage.json', coverage)
        print(json.dumps(coverage), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
