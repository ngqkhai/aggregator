from pydantic_settings import BaseSettings
import os

class Settings(BaseSettings):
    """Application settings for Aggregator service"""
    # RabbitMQ
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    VOICE_RESULTS_EXCHANGE: str = "voice_results"
    VOICE_RESULTS_ROUTING_KEY: str = "voice.generated"
    VOICE_RESULTS_QUEUE: str = "voice_results"
    IMAGE_RESULTS_EXCHANGE: str = "image_results"
    IMAGE_RESULTS_ROUTING_KEY: str = "image.generated"
    IMAGE_RESULTS_QUEUE: str = "image_results"
    SCRIPT_EVENTS_EXCHANGE: str = "script_events"
    SCRIPT_EVENTS_ROUTING_KEY: str = "script.ready"
    SCRIPT_EVENTS_QUEUE: str = "script_events"

    # MongoDB
    MONGODB_URL: str
    MONGODB_DB: str = "script_generator"
    MONGODB_COLLECTION: str = "scripts"

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings() 