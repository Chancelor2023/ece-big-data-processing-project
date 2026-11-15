"""Independent runner around Brett's reviewed Q1–Q8 analysis functions."""
import argparse
import dataclasses
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType
from imdb_pipeline.config import PROJECT_ROOT, CLEAN_DIR
from imdb_pipeline.languages import require_final_language_coverage
from imdb_pipeline.spark import create_spark
from imdb_pipeline.tables import load_clean_tables
from jobs.preparation.prepare_titles import atomic_json
from jobs.exploration import analyses as A, params as P


def csv_ready(frame):
    """Preserve typed Parquet; explicitly JSON-encode nested CSV columns."""
    return frame.select(*[F.to_json(F.col(field.name)).alias(field.name)
        if isinstance(field.dataType, (ArrayType, MapType, StructType)) else F.col(field.name)
        for field in frame.schema.fields])


def write_output(spark, frame, output, name):
    previously_cached = frame.is_cached
    cached = frame if previously_cached else frame.persist(StorageLevel.DISK_ONLY)
    try:
        count = cached.count()
        root = output / name
        cached.write.parquet(str(root / 'parquet'))
        csv_ready(cached).coalesce(1).write.option('header', True).option('escape', '"').csv(str(root / 'csv'))
        if spark.read.parquet(str(root / 'parquet')).count() != count:
            raise ValueError('Output read-back count differs: ' + name)
        print(json.dumps({'output': name, 'rows': count}), flush=True)
        return {'rows': count, 'parquet': name + '/parquet', 'csv': name + '/csv'}
    finally:
        if not previously_cached:
            cached.unpersist()


def load_language_publication(spark, catalog, root=None):
    base = Path(root or PROJECT_ROOT / 'data/languages').resolve()
    pointer = json.loads((base / 'current.json').read_text())
    clean = (base / pointer['path']).resolve()
    if not clean.is_relative_to(base):
        raise ValueError('Language pointer leaves its root')
    coverage = json.loads((clean / 'coverage.json').read_text())
    require_final_language_coverage(coverage, catalog, P.Q6_SUCCESS_RATING, P.Q6_SUCCESS_MIN_VOTES)
    languages = spark.read.parquet(str(clean / 'languages'))
    if set(('tconst', 'language')) - set(languages.columns):
        raise ValueError('Q6 requires the (tconst, language) table')
    if languages.count() != coverage['film_language_pairs']:
        raise ValueError('Q6 published language count differs')
    return languages, coverage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--q6-mode', choices=('final', 'pending'), default='final')
    parser.add_argument('--questions', default='Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8')
    parser.add_argument('--run-id')
    args = parser.parse_args()
    questions = set(args.questions.upper().split(','))
    if not questions or questions - {'Q' + str(i) for i in range(1, 9)}:
        parser.error('Unknown question list')
    run = args.run_id or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:6]
    if not re.fullmatch(r'[A-Za-z0-9_-]+', run):
        parser.error('Invalid run ID')
    source_bytes = (CLEAN_DIR / 'current.json').read_bytes()
    catalog = json.loads(source_bytes)['run_id']
    output = PROJECT_ROOT / 'results/independent_exploration' / run
    output.mkdir(parents=True, exist_ok=False)
    summary = {'run_id': run, 'source_catalog': catalog, 'status': 'running',
               'questions': sorted(questions), 'q6_mode': args.q6_mode, 'outputs': {},
               'analysis_origin': 'Brett273, commit 3ad08a7; independent runner and P364 integration',
               'parameters': dataclasses.asdict(P.Params()),
               'analysis_parameters': {k: getattr(P, k) for k in dir(P) if re.match(r'Q[4-8]_', k)}}
    atomic_json(output / 'run.json', summary)
    spark = create_spark('independent-imdb-exploration')
    try:
        t = load_clean_tables(spark, names=('title.basics', 'title.ratings', 'title.crew',
              'title.episode', 'title.principals', 'name.basics'))
        b, r, c, e, p, n = (t[k] for k in ('title.basics', 'title.ratings', 'title.crew',
                                             'title.episode', 'title.principals', 'name.basics'))
        langs = None
        if 'Q6' in questions:
            if args.q6_mode == 'final':
                langs, summary['language_coverage'] = load_language_publication(spark, catalog)
                expected = A.q6_eligible_films(b, r, P.Q6_SUCCESS_RATING, P.Q6_SUCCESS_MIN_VOTES).count()
                if expected != summary['language_coverage']['eligible_films']:
                    raise ValueError('Q6 coverage denominator differs from current eligible films')
            else:
                summary['q6_status'] = 'pending'
        def emit(name, frame):
            summary['outputs'][name] = write_output(spark, frame, output, name)
            atomic_json(output / 'run.json', summary)
        if 'Q1' in questions:
            emit('q1_runtime_outliers', A.compute_q1(b, P.Params()))
        if questions & {'Q2', 'Q3'}:
            stats = A.genre_year_stats(b, r, P.Params()).persist(StorageLevel.DISK_ONLY)
            try:
                if 'Q2' in questions:
                    emit('q2_top_genres', A.compute_q2(stats, P.Params()))
                if 'Q3' in questions:
                    emit('q3_top_vs_rest', A.compute_q3(stats, P.Params()))
            finally:
                stats.unpersist()
        if 'Q4' in questions:
            emit('q4_one_hit_directors', A.one_hit_directors(b, r, c, n,
                 P.Q4_HIGH_RATING, P.Q4_LOW_AVG_RATING, P.Q4_MIN_OTHER_MOVIES, P.Q4_MIN_VOTES))
        if 'Q5' in questions:
            emit('q5_declining_series', A.declining_series(b, r, e,
                 P.Q5_MIN_FULL_SEASONS, P.Q5_MIN_EPISODES_PER_SEASON, P.Q5_MIN_EPISODE_VOTES))
        if 'Q6' in questions and langs is not None:
            q6 = A.multilingual_actors(b, r, p, langs, n,
                 P.Q6_SUCCESS_RATING, P.Q6_SUCCESS_MIN_VOTES, P.Q6_MIN_LANGUAGES).persist(StorageLevel.DISK_ONLY)
            try:
                emit('q6_multilingual_actors', q6)
                emit('q6_actors_3plus_languages', A.q6_actors_3plus_languages(q6))
            finally:
                q6.unpersist()
            summary['q6_status'] = 'passed'
        if 'Q7' in questions:
            emit('q7_early_peak_actors', A.early_peak_actors(b, r, p, n,
                 P.Q7_MIN_CAREER_YEARS, P.Q7_MIN_MOVIES, P.Q7_MIN_VOTES))
        if 'Q8' in questions:
            person, combinations = A.multi_talented(b, r, c, p, n, P.Q8_MIN_VOTES)
            emit('q8_multi_talented_people', person)
            emit('q8_multi_talented_combinations', combinations)
        if (CLEAN_DIR / 'current.json').read_bytes() != source_bytes:
            raise ValueError('IMDb catalog changed during exploration')
        summary['status'] = 'partial' if summary.get('q6_status') == 'pending' else 'passed'
        summary['full_q1_q8_complete'] = summary['status'] == 'passed' and len(questions) == 8
    except Exception as error:
        summary['status'] = 'failed'
        summary['error'] = str(error)
        raise
    finally:
        summary['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
        atomic_json(output / 'run.json', summary)
        spark.stop()
    print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    main()
