"""Produce factual outputs for the user-selected actor–director question."""
import argparse
import json
from datetime import datetime, timezone
from uuid import uuid4
from pyspark import StorageLevel
from imdb_pipeline.config import PROJECT_ROOT, CLEAN_DIR
from imdb_pipeline.spark import create_spark
from imdb_pipeline.tables import load_clean_tables
from imdb_pipeline.collaborations import collaboration_frames
from jobs.preparation.prepare_titles import atomic_json
from jobs.exploration.run import write_output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--min-votes', type=int, default=1000)
    parser.add_argument('--year-from', type=int, default=1980)
    parser.add_argument('--year-to', type=int, default=2025)
    args = parser.parse_args()
    if args.min_votes < 0 or args.year_from > args.year_to:
        parser.error('Invalid scope')
    catalog_bytes = (CLEAN_DIR / 'current.json').read_bytes()
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:6]
    root = PROJECT_ROOT / 'results/independent_open_question' / run
    root.mkdir(parents=True, exist_ok=False)
    summary = {'run_id': run, 'source_catalog': json.loads(catalog_bytes)['run_id'],
               'status': 'running', 'parameters': vars(args), 'outputs': {},
               'question': 'How do ratings of the first observed year of actor–director collaborations compare to later collaboration years?',
               'unit': 'distinct actor–director pair; first year measured within the declared eligible sample',
               'within_year_order': 'All movies in the first observed year remain in the first group',
               'results_are_associational': True}
    atomic_json(root / 'run.json', summary)
    spark = create_spark('actor-director-collaborations')
    try:
        t = load_clean_tables(spark, names=('title.basics', 'title.ratings', 'title.crew',
                                          'title.principals', 'name.basics'))
        frames = collaboration_frames(t['title.basics'], t['title.ratings'], t['title.crew'],
            t['title.principals'], t['name.basics'], args.min_votes, args.year_from, args.year_to)
        pairs = frames[0].persist(StorageLevel.DISK_ONLY)
        try:
            for name, frame in zip(('pair_comparison', 'pair_movie_observations', 'paired_summary', 'sample_coverage'), frames):
                summary['outputs'][name] = write_output(spark, frame, root, name)
                atomic_json(root / 'run.json', summary)
        finally:
            pairs.unpersist()
        if summary['outputs']['pair_comparison']['rows'] == 0:
            raise ValueError('No repeated actor–director pairs; inspect scope')
        if (CLEAN_DIR / 'current.json').read_bytes() != catalog_bytes:
            raise ValueError('IMDb catalog changed during open question')
        summary['status'] = 'passed'
    except Exception as error:
        summary['status'] = 'failed'
        summary['error'] = str(error)
        raise
    finally:
        summary['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
        atomic_json(root / 'run.json', summary)
        spark.stop()
    print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    main()
