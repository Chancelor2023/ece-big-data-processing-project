"""Select all Q6-eligible films with Spark; no enrichment top-ten/year restriction."""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4
from pyspark import StorageLevel
from imdb_pipeline.config import PROJECT_ROOT, CLEAN_DIR
from imdb_pipeline.spark import create_spark
from imdb_pipeline.tables import load_clean_tables
from jobs.preparation.prepare_titles import atomic_json
from jobs.exploration.analyses import q6_eligible_films


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--min-rating', type=float, default=7.0)
    parser.add_argument('--min-votes', type=int, default=1000)
    args = parser.parse_args()
    if not 0 <= args.min_rating <= 10 or args.min_votes < 0:
        parser.error('Invalid success thresholds')
    source_bytes = (CLEAN_DIR / 'current.json').read_bytes()
    source = json.loads(source_bytes)
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:6]
    root = PROJECT_ROOT / 'data/languages/runs' / run
    root.mkdir(parents=True, exist_ok=False)
    spark = create_spark('q6-select-all-language-targets')
    try:
        tables = load_clean_tables(spark)
        targets = q6_eligible_films(tables['title.basics'], tables['title.ratings'],
                                    args.min_rating, args.min_votes).persist(StorageLevel.DISK_ONLY)
        count = targets.count()
        if count == 0:
            raise ValueError('No eligible Q6 films')
        targets.write.parquet(str(root / 'targets'))
        targets.orderBy('tconst').coalesce(1).write.json(str(root / 'requests'))
        if spark.read.parquet(str(root / 'targets')).count() != count:
            raise ValueError('Target read-back count differs')
        if (CLEAN_DIR / 'current.json').read_bytes() != source_bytes:
            raise ValueError('IMDb catalog changed during selection')
        request_files = sorted((root / 'requests').glob('part-*.json'))
        if len(request_files) != 1:
            raise ValueError('Expected one ordered target stream')
        manifest = {'run_id': run, 'source_catalog': source['run_id'],
                    'catalog_sha256': hashlib.sha256(source_bytes).hexdigest(),
                    'min_rating': args.min_rating, 'min_votes': args.min_votes,
                    'eligible_films': count, 'request_path': 'requests/' + request_files[0].name,
                    'request_sha256': hashlib.sha256(request_files[0].read_bytes()).hexdigest(),
                    'created_at_utc': datetime.now(timezone.utc).isoformat(),
                    'scope': 'All titleType=movie with averageRating>=threshold and numVotes>=threshold'}
        atomic_json(root / 'selection.json', manifest)
        atomic_json(PROJECT_ROOT / 'data/languages/current_selection.json', {'run_id': run})
        print(json.dumps(manifest), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
