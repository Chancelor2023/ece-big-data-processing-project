"""Compare two exploration deliveries without modifying either result set."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, NumericType, StringType
from imdb_pipeline.config import PROJECT_ROOT
from imdb_pipeline.spark import create_spark
from jobs.exploration.run import write_output
from jobs.preparation.prepare_titles import atomic_json

KEYS = {
    'q1_runtime_outliers': ['tconst', 'genre'], 'q2_top_genres': ['startYear', 'genre'],
    'q3_top_vs_rest': ['startYear', 'group'], 'q4_one_hit_directors': ['nconst'],
    'q5_declining_series': ['parentTconst'], 'q6_multilingual_actors': ['nconst'],
    'q6_actors_3plus_languages': ['nconst'], 'q7_early_peak_actors': ['nconst'],
    'q8_multi_talented_people': ['nconst'], 'q8_multi_talented_combinations': ['combination'],
}


def differences(left, right, keys, tolerance=1e-6):
    """Outer-key comparison, supporting typed arrays and their JSON CSV export."""
    if set(keys) - set(left.columns) or set(keys) - set(right.columns):
        raise ValueError('Comparison key missing')
    for frame in (left, right):
        if frame.filter(F.expr(' OR '.join(f'`{k}` IS NULL' for k in keys))).limit(1).count():
            raise ValueError('Null comparison key')
        if frame.groupBy(*keys).count().filter('count != 1').limit(1).count():
            raise ValueError('Duplicate comparison key')
    common = sorted(set(left.columns) & set(right.columns) - set(keys))
    a, b = left.withColumn('_exists_a', F.lit(1)).alias('a'), right.withColumn('_exists_b', F.lit(1)).alias('b')
    joined = a.join(b, [F.col('a.' + k).eqNullSafe(F.col('b.' + k)) for k in keys], 'full')
    mismatch = F.lit(False)
    for name in common:
        l, r = F.col('a.' + name), F.col('b.' + name)
        lt, rt = left.schema[name].dataType, right.schema[name].dataType
        if isinstance(lt, ArrayType) or isinstance(rt, ArrayType):
            dtype = lt if isinstance(lt, ArrayType) else rt
            l = F.from_json(l, dtype) if isinstance(lt, StringType) else l
            r = F.from_json(r, dtype) if isinstance(rt, StringType) else r
            unequal = ~F.to_json(F.sort_array(l)).eqNullSafe(F.to_json(F.sort_array(r)))
        elif isinstance(lt, NumericType) and isinstance(rt, NumericType):
            unequal = F.when(l.isNull() & r.isNull(), False).otherwise(
                F.coalesce(F.abs(l.cast('double') - r.cast('double')) > tolerance, F.lit(True)))
        else:
            unequal = ~l.cast('string').eqNullSafe(r.cast('string'))
        mismatch = mismatch | unequal
    state = (F.when(F.col('a._exists_a').isNull(), 'only_candidate')
              .when(F.col('b._exists_b').isNull(), 'only_reference')
              .when(mismatch, 'different_values').otherwise('equal'))
    return joined.select(*[F.coalesce(F.col('a.' + k), F.col('b.' + k)).alias(k) for k in keys],
                         state.alias('comparison_status'))


def read_output(spark, root, name):
    path = root / name
    if (path / 'parquet').is_dir():
        return spark.read.parquet(str(path / 'parquet'))
    if list(path.glob('*.parquet')):
        return spark.read.parquet(str(path))
    csv_path = path / 'csv' if (path / 'csv').is_dir() else path
    return spark.read.option('header', True).option('inferSchema', True).csv(str(csv_path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--reference-catalog')
    parser.add_argument('--reference-parameters', type=Path)
    parser.add_argument('--questions', default='Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8')
    args = parser.parse_args()
    questions = set(args.questions.upper().split(','))
    if not questions or questions - {'Q' + str(i) for i in range(1, 9)}:
        parser.error('Unknown comparison questions')
    candidate = json.loads((args.candidate / 'run.json').read_text())
    ref_manifest = args.reference / 'run.json'
    reference = json.loads(ref_manifest.read_text()) if ref_manifest.exists() else {}
    catalog = reference.get('source_catalog') or args.reference_catalog
    if not catalog:
        parser.error('Legacy reference needs --reference-catalog; do not infer compatibility')
    ref_parameters = reference.get('parameters')
    if args.reference_parameters:
        ref_parameters = json.loads(args.reference_parameters.read_text())
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:6]
    root = PROJECT_ROOT / 'results/exploration_comparisons' / run
    root.mkdir(parents=True, exist_ok=False)
    summary = {'run_id': run, 'reference': str(args.reference), 'candidate': str(args.candidate),
               'catalogs_match': catalog == candidate['source_catalog'],
               'parameters_documented': ref_parameters is not None,
               'parameters_match': ref_parameters == candidate.get('parameters') if ref_parameters is not None else None,
               'analysis_parameters_match': reference.get('analysis_parameters') == candidate.get('analysis_parameters')
                   if 'analysis_parameters' in reference else None,
               'questions': sorted(questions), 'outputs': {}, 'missing_outputs': [], 'status': 'running'}
    if not summary['catalogs_match']:
        raise ValueError('Different catalogs; results are not comparable')
    spark = create_spark('compare-independent-and-brett-exploration')
    try:
        for name, keys in KEYS.items():
            if name.split('_')[0].upper() not in questions:
                continue
            if not (args.reference / name).exists() or not (args.candidate / name).exists():
                summary['missing_outputs'].append(name)
                continue
            left, right = read_output(spark, args.reference, name), read_output(spark, args.candidate, name)
            comparison = differences(left, right, keys)
            counts = {r.comparison_status: r['count'] for r in comparison.groupBy('comparison_status').count().collect()}
            detail = write_output(spark, comparison.filter("comparison_status != 'equal'"), root, name)
            summary['outputs'][name] = {'counts': counts, 'column_sets_match': set(left.columns) == set(right.columns),
                                        'reference_only_columns': sorted(set(left.columns) - set(right.columns)),
                                        'candidate_only_columns': sorted(set(right.columns) - set(left.columns)),
                                        'differences': detail}
        summary['status'] = 'comparison_completed'
        summary['equivalence_confirmed'] = (
            summary['parameters_match'] is True
            and (not (questions & {'Q4', 'Q5', 'Q6', 'Q7', 'Q8'}) or summary['analysis_parameters_match'] is True)
            and not summary['missing_outputs'] and bool(summary['outputs'])
            and all(v['column_sets_match'] and v['differences']['rows'] == 0 for v in summary['outputs'].values()))
        summary['full_q1_q8_equivalence'] = summary['equivalence_confirmed'] and len(questions) == 8
    finally:
        atomic_json(root / 'comparison.json', summary)
        spark.stop()
    print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    main()
