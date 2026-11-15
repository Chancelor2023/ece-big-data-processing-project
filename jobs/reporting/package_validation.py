"""Package factual test/comparison evidence without report explanations."""
import argparse
import csv
import hashlib
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--comparison', type=Path, required=True)
    parser.add_argument('--test-log', type=Path, required=True)
    parser.add_argument('--candidate-tree', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = Path(os.environ.get('PROJECT_ROOT', Path(__file__).resolve().parents[2])).resolve()
    output = (root / args.output).resolve()
    if not output.is_relative_to(root / 'report-assets') or output.exists():
        parser.error('Choose a new report-assets directory')
    if not re.fullmatch('[0-9a-f]{40}', args.candidate_tree):
        parser.error('Expected the tested Git tree identifier')
    comparison = json.loads(args.comparison.read_text(encoding='utf-8'))
    log = args.test_log.read_text(encoding='utf-8', errors='replace')
    result = re.search(r'Ran ([0-9]+) tests in ([0-9.]+)s', log)
    if not result or not re.search(r'^OK\s*$', log, re.M) or re.search(r'^FAILED', log, re.M):
        raise ValueError('Test log does not contain a successful suite summary')
    if comparison.get('status') != 'comparison_completed':
        raise ValueError('Comparison has not completed')
    output.mkdir(parents=True)
    shutil.copyfile(args.comparison, output / 'COMPARISON_Q1_Q3.json')
    if sha(args.comparison) != sha(output / 'COMPARISON_Q1_Q3.json'):
        raise ValueError('Comparison copy differs')
    tests = {'status': 'passed', 'tests': int(result.group(1)), 'seconds': float(result.group(2)),
             'command': 'python -m unittest discover -s tests -v',
             'environment': 'Isolated checkout and Docker container, 4 GB, 2 CPUs',
             'candidate_source_tree': args.candidate_tree, 'test_log_sha256': sha(args.test_log),
             'scope': 'Available automated tests; not a fresh download/recollection of every external dataset',
             'recorded_at_utc': datetime.now(timezone.utc).isoformat()}
    (output / 'TESTS.json').write_text(json.dumps(tests, indent=2) + '\n', encoding='utf-8', newline='\n')
    with (output / 'SOURCES.csv').open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=('section', 'description', 'original_path',
            'packaged_path', 'source_url', 'sha256'), lineterminator='\n')
        writer.writeheader()
        writer.writerow({'section': 'comparison', 'description': 'Actual Q1–Q3 comparison',
            'original_path': args.comparison.resolve().relative_to(root).as_posix(),
            'packaged_path': 'COMPARISON_Q1_Q3.json', 'source_url': '', 'sha256': sha(args.comparison)})
    index = ('# Independent validation evidence\n\n'
             '[Automated tests](TESTS.json) and [Q1–Q3 comparison](COMPARISON_Q1_Q3.json).\n\n'
             f'Tests passed: {tests["tests"]}. Comparison scope: {", ".join(comparison["questions"])}.\n'
             f'Equivalence confirmed for this scope: {comparison["equivalence_confirmed"]}.\n'
             'This does not establish equivalence of the remaining questions.\n\n'
             '[Final query tables](../independent-final-20261115/INDEX.md).\n'
             'Original analysis functions: Brett273, 3ad08a7; Q1–Q3 reference: 69da2b4.\n'
             'Contributors write their own explanations and interpretations in REPORT.md.\n')
    (output / 'INDEX.md').write_text(index, encoding='utf-8', newline='\n')
    payload = sorted(p for p in output.rglob('*') if p.is_file())
    (output / 'SHA256SUMS.txt').write_text('\n'.join(sha(p) + '  ' + p.relative_to(output).as_posix()
        for p in payload) + '\n', encoding='utf-8', newline='\n')
    files = sorted(p for p in output.rglob('*') if p.is_file())
    (output / 'MANIFEST.json').write_text(json.dumps({'files': [
        {'path': p.relative_to(output).as_posix(), 'bytes': p.stat().st_size, 'sha256': sha(p)}
        for p in files]}, indent=2) + '\n', encoding='utf-8', newline='\n')
    print(json.dumps({'status': 'passed', 'tests': tests['tests'], 'output': str(output)}))


if __name__ == '__main__':
    main()
