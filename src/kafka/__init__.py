"""Kafka integration for EduGuard"""
from src.kafka.producer import EduGuardKafkaProducer, get_kafka_producer
from src.kafka.consumer import EduGuardKafkaConsumer, get_kafka_consumer
from src.kafka.schemas import StudentEventPayload

__all__ = [
    "EduGuardKafkaProducer",
    "get_kafka_producer",
    "EduGuardKafkaConsumer",
    "get_kafka_consumer",
    "StudentEventPayload",
]
