"""Create project topics explicitly; safe to rerun without deleting data."""

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from imdb_pipeline.config import KAFKA_BOOTSTRAP_SERVERS, WIKIMEDIA_TOPIC


def main() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    futures = admin.create_topics(
        [NewTopic(WIKIMEDIA_TOPIC, num_partitions=3, replication_factor=1)],
        request_timeout=30,
    )
    for topic, future in futures.items():
        try:
            future.result(timeout=40)
            print(f"Created topic: {topic}")
        except KafkaException as exc:
            from confluent_kafka import KafkaError

            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            print(f"Topic already exists: {topic}")


if __name__ == "__main__":
    main()
