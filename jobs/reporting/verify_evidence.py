"""Verify an evidence bundle's exact bytes without Spark or network access."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath


def verify(bundle):
    bundle = Path(bundle).resolve()

    def target(name):
        relative = PurePosixPath(name)
        path = (bundle / name).resolve()
        if relative.is_absolute() or '..' in relative.parts or not path.is_relative_to(bundle):
            raise ValueError('Unsafe evidence path: ' + name)
        return path

    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    manifest = json.loads((bundle / 'MANIFEST.json').read_text(encoding='utf-8-sig'))
    expected = {item['path'] for item in manifest['files']}
    if len(expected) != len(manifest['files']):
        raise ValueError('Duplicate manifest entries')
    actual = {p.relative_to(bundle).as_posix() for p in bundle.rglob('*') if p.is_file()}
    if actual != expected | {'MANIFEST.json'}:
        raise ValueError('Bundle inventory differs from manifest')
    for item in manifest['files']:
        path = target(item['path'])
        if path.stat().st_size != item['bytes'] or digest(path) != item['sha256']:
            raise ValueError('Manifest mismatch: ' + item['path'])
    sums = {}
    for line in (bundle / 'SHA256SUMS.txt').read_text(encoding='utf-8').splitlines():
        sha, name = line.split('  ', 1)
        if name in sums or digest(target(name)) != sha:
            raise ValueError('SHA256SUMS mismatch: ' + name)
        sums[name] = sha
    if set(sums) != expected - {'SHA256SUMS.txt'}:
        raise ValueError('SHA256SUMS inventory differs from manifest')
    source_copies = 0
    with (bundle / 'SOURCES.csv').open(encoding='utf-8-sig', newline='') as stream:
        for row in csv.DictReader(stream):
            if row['packaged_path']:
                if digest(target(row['packaged_path'])) != row['sha256']:
                    raise ValueError('Source-copy mismatch: ' + row['packaged_path'])
                source_copies += 1
    csv_files = 0
    for path in bundle.rglob('*.csv'):
        # Parse to ensure record terminators and quoted fields remain readable.
        list(csv.reader(io.StringIO(path.read_text(encoding='utf-8-sig'), newline='')))
        csv_files += 1
    return {'status': 'passed', 'manifest_files': len(expected),
            'checksum_files': len(sums), 'source_copies': source_copies,
            'csv_files': csv_files, 'bundle': str(bundle)}


def main():
    root = Path(os.environ.get('PROJECT_ROOT', Path(__file__).resolve().parents[2]))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, default=root / 'report-assets/evidence-20261115')
    args = parser.parse_args()
    print(json.dumps(verify(args.bundle)), flush=True)


if __name__ == '__main__':
    main()
