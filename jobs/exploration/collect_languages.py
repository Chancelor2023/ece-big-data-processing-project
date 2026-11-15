"""Serial, cached, resumable raw P345/P364 collection; no text generation."""
import argparse
import hashlib
import itertools
import json
import math
import re
from datetime import datetime, timezone
from imdb_pipeline.config import PROJECT_ROOT
from imdb_pipeline.wikimedia import WikimediaClient, save_json


def make_query(ids):
    if not ids or len(ids) > 40 or any(not re.fullmatch(r'tt[0-9]+', x) for x in ids):
        raise ValueError('Expected 1–40 exact IMDb title identifiers')
    values = ' '.join(json.dumps(x) for x in ids)
    return ('PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n'
            'SELECT DISTINCT ?imdb ?item ?language WHERE { VALUES ?imdb { ' + values + ' } '
            'OPTIONAL { ?item wdt:P345 ?imdb . OPTIONAL { ?item wdt:P364 ?language . } } }')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id')
    parser.add_argument('--batch-size', type=int, default=40)
    parser.add_argument('--max-batches', type=int)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 40 or (args.max_batches is not None and args.max_batches < 1):
        parser.error('Batch size must be 1–40; optional max-batches must be positive')
    base = PROJECT_ROOT / 'data/languages'
    run = args.run_id or json.loads((base / 'current_selection.json').read_text())['run_id']
    if not re.fullmatch(r'[A-Za-z0-9_-]+', run):
        parser.error('Invalid run ID')
    root = base / 'runs' / run
    selection = json.loads((root / 'selection.json').read_text())
    requests_path = root / selection['request_path']
    if hashlib.sha256(requests_path.read_bytes()).hexdigest() != selection['request_sha256']:
        raise ValueError('Language request stream changed')
    guard = root / 'collection_configuration.json'
    config = {'batch_size': args.batch_size, 'request_sha256': selection['request_sha256'],
              'endpoint': 'https://query.wikidata.org/sparql', 'source_catalog': selection['source_catalog']}
    if guard.exists() and json.loads(guard.read_text()) != config:
        raise ValueError('Collection configuration changed; use the original batch size')
    save_json(guard, config)
    client = WikimediaClient(base / 'http_cache', interval=1.0)
    expected_batches = math.ceil(selection['eligible_films'] / args.batch_size)
    progress = {'run_id': run, 'status': 'running', 'eligible_films': selection['eligible_films'],
                'expected_batches': expected_batches, 'processed_films': 0,
                'completed_batches': 0, 'new_requests': 0, 'reused_batches': 0}
    try:
        with requests_path.open(encoding='utf-8') as stream:
            identifiers = (json.loads(line)['tconst'] for line in stream if line.strip())
            for batch_id in range(expected_batches):
                if args.max_batches is not None and batch_id >= args.max_batches:
                    break
                ids = list(itertools.islice(identifiers, args.batch_size))
                query = make_query(ids)
                raw = root / 'raw' / f'{batch_id:06d}.json'
                if raw.exists():
                    saved = json.loads(raw.read_text())
                    if saved.get('target_ids') != ids or not saved.get('response', {}).get('raw_response'):
                        raise ValueError('Existing language batch differs from selection')
                    progress['reused_batches'] += 1
                else:
                    response = client.get(config['endpoint'], {'query': query, 'format': 'json'})
                    save_json(raw, {'batch_id': batch_id, 'target_ids': ids, 'response': response})
                    progress['new_requests'] += 1
                progress['processed_films'] += len(ids)
                progress['completed_batches'] += 1
                progress['updated_at_utc'] = datetime.now(timezone.utc).isoformat()
                save_json(root / 'collection.json', progress)
                print(json.dumps(progress), flush=True)
        progress['status'] = 'completed' if progress['processed_films'] == selection['eligible_films'] else 'partial'
    except KeyboardInterrupt:
        progress['status'] = 'interrupted'
        raise
    except Exception as error:
        progress['status'] = 'failed'
        progress['error'] = str(error)
        raise
    finally:
        progress['updated_at_utc'] = datetime.now(timezone.utc).isoformat()
        save_json(root / 'collection.json', progress)
    print(json.dumps(progress), flush=True)


if __name__ == '__main__':
    main()
