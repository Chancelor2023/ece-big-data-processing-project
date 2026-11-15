"""Regenerate only fallback CSV exports from authoritative typed Parquet."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from imdb_pipeline.config import PROJECT_ROOT
from imdb_pipeline.spark import create_spark
from jobs.preparation.prepare_titles import atomic_json
from jobs.exploration.run import csv_ready


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-root', type=Path, action='append', required=True)
    args = parser.parse_args()
    allowed = [(PROJECT_ROOT / 'results/independent_exploration').resolve(),
               (PROJECT_ROOT / 'results/independent_open_question').resolve()]
    for root in args.run_root:
        if not any(root.resolve().is_relative_to(base) for base in allowed):
            parser.error('Only independent generated query outputs may be reformatted')
    spark = create_spark('canonical-query-csv-exports')
    try:
        for root in args.run_root:
            root = root.resolve()
            manifest = json.loads((root / 'run.json').read_text())
            if manifest['status'] != 'passed':
                raise ValueError('Require a completed query run')
            if manifest.get('csv_format') == 'RFC4180; nested values JSON; UTF-8':
                print(json.dumps({'run_root': str(root), 'status': 'already_canonical'}), flush=True)
                continue
            for name, info in manifest['outputs'].items():
                parquet = (root / info['parquet']).resolve()
                destination = (root / info['csv']).resolve()
                if not parquet.is_relative_to(root) or not destination.is_relative_to(root):
                    raise ValueError('Output path leaves its run')
                frame = spark.read.parquet(str(parquet))
                temporary = destination.with_name('csv-rfc4180')
                backup = destination.with_name('csv-before-rfc4180')
                if temporary.exists() or backup.exists():
                    raise ValueError('Formatting scratch/backup exists; preserve and inspect it')
                csv_ready(frame).coalesce(1).write.option('header', True).option('escape', '"').csv(str(temporary))
                count = spark.read.option('header', True).option('escape', '"').csv(str(temporary)).count()
                if count != info['rows']:
                    raise ValueError('CSV record count differs: ' + name)
                # Both absolute targets were checked inside this generated run.
                destination.rename(backup)
                temporary.rename(destination)
                print(json.dumps({'output': name, 'rows': count, 'status': 'canonical'}), flush=True)
            manifest['csv_format'] = 'RFC4180; nested values JSON; UTF-8'
            manifest['csv_verified_at_utc'] = datetime.now(timezone.utc).isoformat()
            atomic_json(root / 'run.json', manifest)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
