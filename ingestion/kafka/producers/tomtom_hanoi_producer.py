"""
Backward-compatible wrapper for the generic TomTom producer.

Use ingestion/kafka/producers/tomtom_producer.py for new commands/imports.
"""
from ingestion.kafka.producers.tomtom_producer import *  # noqa: F401,F403
from ingestion.kafka.producers.tomtom_producer import main


if __name__ == "__main__":
    main()
