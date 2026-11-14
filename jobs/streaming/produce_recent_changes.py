"""Publish real English Wikipedia article edits to Kafka with an acknowledged SSE cursor."""

import argparse
import json
import re
import signal
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from imdb_pipeline.config import CHECKPOINT_DIR, KAFKA_BOOTSTRAP_SERVERS, RESULTS_DIR, WIKIMEDIA_TOPIC
from imdb_pipeline.recent_changes import EVENTSTREAM_URL, STREAM_USER_AGENT, eligible_change, sse_messages
from jobs.preparation.prepare_titles import atomic_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=int, default=0)
    parser.add_argument("--state-name", default="recentchange-enwiki")
    parser.add_argument("--topic", default=WIKIMEDIA_TOPIC)
    args = parser.parse_args()
    if args.duration_seconds < 0 or not re.fullmatch(r"[A-Za-z0-9_-]+", args.state_name):
        parser.error("Invalid duration or state name")
    state_path = CHECKPOINT_DIR / "streaming" / args.state_name / "producer.json"
    config = {"endpoint": EVENTSTREAM_URL, "topic": args.topic, "brokers": KAFKA_BOOTSTRAP_SERVERS, "scope": "enwiki namespace 0 edit/new; bots included"}
    state = json.loads(state_path.read_text()) if state_path.exists() else {"config": config}
    if state["config"] != config:
        raise ValueError("Producer configuration changed: choose a new --state-name")
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "enable.idempotence": True,
        "acks": "all", "delivery.timeout.ms": 30000,
    })
    session = requests.Session()
    session.headers.update({"User-Agent": STREAM_USER_AGENT, "Accept": "text/event-stream"})
    started = time.monotonic()
    stopping = False
    stats = {"published": 0, "connections": 0, "malformed_frames": 0, "status": "running", "started_at_utc": datetime.now(timezone.utc).isoformat(), "synthetic": False}

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def expired():
        return stopping or (args.duration_seconds and time.monotonic() - started >= args.duration_seconds)

    def delivery(error, message):
        if error:
            delivery_errors.append(str(error))

    try:
        while not expired():
            headers = {"Last-Event-ID": state["last_event_id"]} if state.get("last_event_id") else {}
            try:
                with session.get(EVENTSTREAM_URL, headers=headers, stream=True, timeout=(15, 30)) as response:
                    if response.status_code == 429:
                        delay = max(5, int(response.headers.get("Retry-After", "30")))
                        for _ in range(delay):
                            if expired():
                                break
                            time.sleep(1)
                        continue
                    response.raise_for_status()
                    response.encoding = "utf-8"
                    stats["connections"] += 1
                    for cursor, event_type, data in sse_messages(response.iter_lines(decode_unicode=True, chunk_size=1024)):
                        if expired():
                            break
                        if event_type != "message":
                            continue
                        try:
                            event = json.loads(data)
                        except json.JSONDecodeError:
                            stats["malformed_frames"] += 1
                            continue
                        if not eligible_change(event):
                            continue
                        delivery_errors = []
                        producer.produce(args.topic, key=event["meta"]["id"], value=data, on_delivery=delivery)
                        if producer.flush(30) or delivery_errors:
                            raise RuntimeError(f"Kafka delivery failed: {delivery_errors}")
                        # Only advance the SSE resume cursor after broker acknowledgement.
                        if cursor:
                            state.update(last_event_id=cursor, acknowledged_at_utc=datetime.now(timezone.utc).isoformat())
                            atomic_json(state_path, state)
                        stats["published"] += 1
                        if stats["published"] % 25 == 0:
                            atomic_json(RESULTS_DIR / "streaming" / "producer" / f"{args.state_name}.json", stats)
                            print(json.dumps(stats), flush=True)
            except requests.RequestException as error:
                stats["last_network_error"] = str(error)
                print(f"EventStreams reconnect: {error}", flush=True)
                for _ in range(5):
                    if expired():
                        break
                    time.sleep(1)
        stats["status"] = "stopped"
    except Exception as error:
        stats.update(status="failed", error=str(error))
        raise
    finally:
        producer.flush(30)
        session.close()
        stats["elapsed_seconds"] = round(time.monotonic() - started, 2)
        atomic_json(RESULTS_DIR / "streaming" / "producer" / f"{args.state_name}.json", stats)
        print(json.dumps(stats), flush=True)


if __name__ == "__main__":
    main()
