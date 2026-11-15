"""Package factual fallback query outputs and independent read-back checks."""
import argparse
import csv
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import functions as F
from imdb_pipeline.config import PROJECT_ROOT
from imdb_pipeline.spark import create_spark
from jobs.preparation.prepare_titles import atomic_json
from jobs.exploration.run import csv_ready


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--exploration-run', required=True)
    parser.add_argument('--open-run', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    root = PROJECT_ROOT.resolve()
    output = (root / args.output).resolve()
    if not output.is_relative_to(root / 'report-assets') or output.exists():
        parser.error('Choose a new directory under report-assets')
    exploration = root / 'results/independent_exploration' / args.exploration_run
    open_root = root / 'results/independent_open_question' / args.open_run
    runs = {'exploration': (exploration, json.loads((exploration / 'run.json').read_text())),
            'open_question': (open_root, json.loads((open_root / 'run.json').read_text()))}
    if not all(m['status'] == 'passed' for _, m in runs.values()) or not runs['exploration'][1]['full_q1_q8_complete']:
        raise ValueError('Require completed exploration and open-question runs')
    catalog = runs['exploration'][1]['source_catalog']
    if runs['open_question'][1]['source_catalog'] != catalog:
        raise ValueError('Output catalogs differ')
    language_base = root / 'data/languages'
    pointer = json.loads((language_base / 'current.json').read_text())
    language_root = language_base / pointer['path']
    coverage = json.loads((language_root / 'coverage.json').read_text())
    if coverage['source_catalog'] != catalog:
        raise ValueError('Language source catalog differs')
    protected = {name: digest(root / name) for name in ('PROJECT.md', 'REPORT.md')}
    output.mkdir(parents=True)
    checks = {}
    sources = []
    def copy_source(source, destination, category):
        shutil.copyfile(source, destination)
        if digest(source) != digest(destination):
            raise ValueError('Copied result changed: ' + str(source))
        sources.append({'section': category, 'description': 'Validated result copy',
            'original_path': source.relative_to(root).as_posix(),
            'packaged_path': destination.relative_to(output).as_posix(),
            'source_url': '', 'sha256': digest(source)})
    def check(name, condition):
        checks[name] = bool(condition)
        if not condition:
            raise ValueError('Final-output check failed: ' + name)
    spark = create_spark('export-validated-fallback-results')
    try:
        frames = {}
        for category, (source, manifest) in runs.items():
            target = output / category; target.mkdir()
            copy_source(source / 'run.json', target / 'run.original.json', category)
            for name, metadata in manifest['outputs'].items():
                frame = spark.read.parquet(str(source / metadata['parquet']))
                check(category + '_' + name + '_rows', frame.count() == metadata['rows'])
                frames[name] = frame
                csv_files = list((source / metadata['csv']).glob('part-*.csv'))
                if len(csv_files) != 1:
                    raise ValueError('Expected one final CSV: ' + name)
                copy_source(csv_files[0], target / (name + '.csv'), category)
        q2 = frames['q2_top_genres']
        check('q2_three_per_year', q2.groupBy('startYear').count().filter('count != 3').count() == 0)
        check('q2_full_1980_2025', q2.select('startYear').distinct().count() == 46
              and q2.filter('startYear < 1980 OR startYear > 2025').count() == 0)
        q3 = frames['q3_top_vs_rest']
        check('q3_shares_sum_100', q3.groupBy('startYear').agg(F.sum('vote_share').alias('total'))
              .filter(F.abs(F.col('total') - 100) > 1e-6).count() == 0)
        check('q3_top_group_three', q3.filter((F.col('group') == 'top') & (F.col('n_genres') != 3)).count() == 0)
        check('q4_thresholds', frames['q4_one_hit_directors'].filter('hit_rating < 8 OR n_other < 2 OR avg_other_rating > 6').count() == 0)
        check('q5_ordinary_seasons', frames['q5_declining_series'].filter(
            'worst_season <= 0 OR best_season <= 0 OR worst_season != last_season OR best_season > second_season').count() == 0)
        q6 = frames['q6_multilingual_actors']; q6three = frames['q6_actors_3plus_languages']
        check('q6_language_counts', q6.filter((F.col('n_languages') < 2) | (F.size('languages') != F.col('n_languages'))).count() == 0)
        check('q6_three_plus_exact', q6.filter('n_languages >= 3').exceptAll(q6three).count() == 0
              and q6three.exceptAll(q6.filter('n_languages >= 3')).count() == 0)
        check('q7_career_thresholds', frames['q7_early_peak_actors'].filter(
            'last_year - first_year < 20 OR n_movies < 15 OR peak_year > first_year + (last_year - first_year) / 2').count() == 0)
        check('q8_role_counts', frames['q8_multi_talented_people'].filter(
            'n_role_types < 2 OR n_multi_role_movies < 1 OR n_single_role_movies < 1').count() == 0)
        pairs = frames['pair_comparison']; paired = frames['paired_summary'].first()
        check('open_pair_counts', pairs.filter('first_year_movies + later_year_movies != pair_movie_count OR later_year_movies <= 0').count() == 0)
        check('open_summary_matches_pairs', paired.pairs == pairs.count()
              and paired.pairs_higher_later + paired.pairs_lower_later + paired.pairs_equal == paired.pairs)
        source_target = output / 'language_sources'; source_target.mkdir()
        copy_source(language_root / 'coverage.json', source_target / 'coverage.original.json', 'languages')
        # Only compact lineage metadata is packaged, not raw query responses.
        lineage = spark.read.parquet(str(language_root / 'languages')).select(
            'tconst', 'language', 'entity_uri', 'language_uri', 'retrieved_at_utc', 'property_uri')
        temporary = source_target / '_spark_export'
        csv_ready(lineage).coalesce(1).write.option('header', True).option('escape', '"').csv(str(temporary))
        part = next(temporary.glob('part-*.csv'))
        shutil.copyfile(part, source_target / 'film_language_lineage.csv')
        # Remove only the explicitly created Spark export scratch directory.
        if temporary.parent != source_target or temporary.name != '_spark_export':
            raise ValueError('Unexpected temporary directory')
        shutil.rmtree(temporary)
        for name, sha in protected.items():
            check(name + '_unchanged', digest(root / name) == sha)
        atomic_json(output / 'VALIDATION.json', {'status': 'passed', 'checks': checks,
                    'source_catalog': catalog, 'language_run': pointer['run_id'],
                    'exploration_run': args.exploration_run, 'open_run': args.open_run,
                    'protected_files': protected})
        with (output / 'SOURCES.csv').open('w', encoding='utf-8', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=('section', 'description', 'original_path',
                'packaged_path', 'source_url', 'sha256'), lineterminator='\n')
            writer.writeheader(); writer.writerows(sources)
        lines = ['# Independent final query outputs', '',
                 'Factual outputs only. Contributors write REPORT.md explanations themselves.', '',
                 '| Output | Rows | CSV |', '| --- | --- | --- |']
        for category, (_, manifest) in runs.items():
            for name, info in manifest['outputs'].items():
                lines.append(f'| {name} | {info["rows"]} | [{name}.csv]({category}/{name}.csv) |')
        lines.extend(['', '[Language coverage](language_sources/coverage.original.json)',
                      '[Language lineage](language_sources/film_language_lineage.csv)',
                      '[Read-back validation](VALIDATION.json)', '[Source files](SOURCES.csv)', '',
                      'Analysis functions originate from Brett273 commit 3ad08a7. The independent runner,',
                      'P364 integration and actor–director question are documented in docs/INDEPENDENT_FINALIZATION.md.',
                      'The first collaboration year is observed within the declared eligible sample;',
                      'all movies in that year are grouped together. A movie can occur in multiple pairs.',
                      'Actor and director roles can belong to the same person; no distinct-person filter is imposed.',
                      'The paired tables report associations, not causal effects.', ''])
        (output / 'INDEX.md').write_text('\n'.join(lines), encoding='utf-8', newline='\n')
        payload = sorted(p for p in output.rglob('*') if p.is_file())
        (output / 'SHA256SUMS.txt').write_text('\n'.join(digest(p) + '  ' + p.relative_to(output).as_posix()
            for p in payload) + '\n', encoding='utf-8', newline='\n')
        files = sorted(p for p in output.rglob('*') if p.is_file())
        manifest = {'source_catalog': catalog, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
                    'files': [{'path': p.relative_to(output).as_posix(), 'bytes': p.stat().st_size,
                               'sha256': digest(p)} for p in files]}
        atomic_json(output / 'MANIFEST.json', manifest)
        print(json.dumps({'status': 'passed', 'output': str(output), 'checks': len(checks),
                          'files': len(files) + 1}), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
