"""Regressions for fallback publication, runner and open-question integration."""
import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from pyspark.sql import SparkSession
from imdb_pipeline.languages import BATCH_SCHEMA, prepare_languages, require_final_language_coverage
from imdb_pipeline.collaborations import collaboration_frames
from jobs.exploration.collect_languages import make_query
from jobs.exploration import run as R
from jobs.exploration.compare import differences


class PublicationGuards(unittest.TestCase):
    def coverage(self):
        return dict(status='passed', source_catalog='catalog', min_rating=7.0, min_votes=1000,
                    collection_complete=True, eligible_films=10, declared_films=2,
                    status_counts={'declared': 2, 'no_p364': 8})

    def test_partial_availability_is_valid_after_complete_collection(self):
        require_final_language_coverage(self.coverage(), 'catalog', 7.0, 1000)

    def test_final_rejects_incomplete_zero_or_foreign_collection(self):
        for changes in ({'collection_complete': False}, {'source_catalog': 'other'},
                        {'declared_films': 0}, {'status_counts': {'declared': 2, 'not_collected': 8}},
                        {'min_votes': 10}, {'status_counts': {'declared': 2}}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                require_final_language_coverage(self.coverage() | changes, 'catalog', 7.0, 1000)

    def test_query_rejects_invalid_ids_and_oversized_batches(self):
        self.assertIn('wdt:P364', make_query(['tt1', 'tt2']))
        for ids in ([], ['tt1'] * 41, ['tt1" } UNION { ?s ?p ?o']):
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                make_query(ids)


class IndependentSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[1]')
                     .config('spark.sql.shuffle.partitions', '2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_raw_languages_distinguish_multilingual_and_ambiguous_films(self):
        def binding(t, entity=None, language=None):
            row = {'imdb': {'value': t}}
            if entity:
                row['item'] = {'value': 'http://www.wikidata.org/entity/' + entity}
            if language:
                row['language'] = {'value': 'http://www.wikidata.org/entity/' + language}
            return row
        rows = [binding('tt1', 'Q1', 'Q150'), binding('tt1', 'Q1', 'Q1860'),
                binding('tt1', 'Q1', 'Q150'), binding('tt2', 'Q2'), binding('tt3'),
                binding('tt4', 'Q4', 'Q150'), binding('tt4', 'Q5', 'Q1860')]
        raw = self.spark.createDataFrame([dict(batch_id=0, target_ids=['tt1', 'tt2', 'tt3', 'tt4'],
            response=dict(url='https://query.wikidata.org/sparql?query=fixture',
                          retrieved_at_utc='2026-11-15T00:00:00Z',
                          raw_response=json.dumps({'results': {'bindings': rows}})))], BATCH_SCHEMA)
        eligible = self.spark.createDataFrame([(f'tt{i}',) for i in range(1, 6)], 'tconst string')
        languages, statuses = prepare_languages(raw, eligible)
        self.assertEqual({r.tconst: r.status for r in statuses.collect()},
                         dict(tt1='declared', tt2='no_p364', tt3='no_wikidata_match',
                              tt4='ambiguous_match', tt5='not_collected'))
        self.assertEqual({(r.tconst, r.language) for r in languages.collect()},
                         {('tt1', 'Q150'), ('tt1', 'Q1860')})
        self.assertTrue(all(r.source_query_url and r.retrieved_at_utc for r in languages.collect()))

    def test_runner_writes_array_languages_as_typed_parquet_and_json_csv(self):
        frame = self.spark.createDataFrame([('a1', ['Q150', 'Q1860'])],
                                           'nconst string, languages array<string>')
        with tempfile.TemporaryDirectory() as directory:
            report = R.write_output(self.spark, frame, Path(directory), 'q6')
            self.assertEqual(report['rows'], 1)
            actual = self.spark.read.parquet(str(Path(directory) / report['parquet'])).first()
            self.assertEqual(actual.languages, ['Q150', 'Q1860'])
            exported = self.spark.read.option('header', True).option('escape', '"').csv(str(Path(directory) / report['csv'])).first()
            self.assertEqual(json.loads(exported.languages), ['Q150', 'Q1860'])
            part = next((Path(directory) / report['csv']).glob('part-*.csv'))
            with part.open(encoding='utf-8', newline='') as stream:
                portable = next(csv.DictReader(stream))
            self.assertEqual(json.loads(portable['languages']), ['Q150', 'Q1860'])

    def test_runner_pending_q6_does_not_require_a_language_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); clean = root / 'data/clean'; clean.mkdir(parents=True)
            (clean / 'current.json').write_text(json.dumps({'run_id': 'catalog'}))
            frames = {name: None for name in ('title.basics', 'title.ratings', 'title.crew',
                                               'title.episode', 'title.principals', 'name.basics')}
            with patch.object(R, 'PROJECT_ROOT', root), patch.object(R, 'CLEAN_DIR', clean), \
                 patch.object(R, 'create_spark', return_value=self.spark), \
                 patch.object(R, 'load_clean_tables', return_value=frames), \
                 patch.object(self.spark, 'stop'), \
                 patch('sys.argv', ['run', '--questions', 'Q6', '--q6-mode', 'pending', '--run-id', 'pending-test']):
                R.main()
            summary = json.loads((root / 'results/independent_exploration/pending-test/run.json').read_text())
            self.assertEqual(summary['status'], 'partial')
            self.assertEqual(summary['q6_status'], 'pending')
            self.assertFalse(summary['full_q1_q8_complete'])

    def test_comparison_detects_missing_keys_and_numeric_differences(self):
        reference = self.spark.createDataFrame([('a', 7.0), ('b', 8.0), ('d', 9.0)],
                                                'id string, rating double')
        candidate = self.spark.createDataFrame([('a', 7.00000001), ('b', 6.0), ('c', 9.0)],
                                                'id string, rating double')
        result = {r.id: r.comparison_status for r in differences(reference, candidate, ['id']).collect()}
        self.assertEqual(result, dict(a='equal', b='different_values',
                                     c='only_candidate', d='only_reference'))

    def test_collaboration_first_year_keeps_ties_and_deduplicates_credits(self):
        b = self.spark.createDataFrame([
            ('m1', 'movie', 'First A', 2000), ('m2', 'movie', 'First B', 2000),
            ('m3', 'movie', 'Later', 2001), ('m4', 'movie', 'Excluded', 2002)],
            'tconst string, titleType string, primaryTitle string, startYear int')
        r = self.spark.createDataFrame([('m1', 5.0, 1000), ('m2', 7.0, 1000),
                                       ('m3', 8.0, 1000), ('m4', 1.0, 10)],
                                       'tconst string, averageRating double, numVotes long')
        c = self.spark.createDataFrame([(m, ['d1', 'd1']) for m in ('m1', 'm2', 'm3', 'm4')],
                                       'tconst string, directors array<string>')
        p = self.spark.createDataFrame([('m1', 'a1', 'actor'), ('m1', 'a1', 'actress'),
                ('m2', 'a1', 'actor'), ('m3', 'a1', 'actor'), ('m4', 'a1', 'actor'),
                ('m1', 'a2', 'actor'), ('m2', 'a2', 'actor')],
                'tconst string, nconst string, category string')
        n = self.spark.createDataFrame([('a1', 'Actor 1'), ('a2', 'Actor 2'), ('d1', 'Director')],
                                       'nconst string, primaryName string')
        pairs, observations, summary, coverage = collaboration_frames(b, r, c, p, n)
        row = pairs.first()
        self.assertEqual((row.actor_id, row.pair_movie_count, row.first_year_movies, row.later_year_movies),
                         ('a1', 3, 2, 1))
        self.assertEqual(row.rating_difference, 2.0)
        self.assertEqual(observations.count(), 3)
        self.assertEqual(summary.first().pairs, 1)
        self.assertEqual(coverage.first().eligible_credited_movies, 3)


if __name__ == '__main__':
    unittest.main()
