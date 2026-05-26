from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    broker_address: str = "localhost:9092"
    metrics_topic: str = "__quix_metrics"
    consumer_group: str = "quix-dashboard-backend"
    sse_interval: float = 5.0

    model_config = {"env_prefix": "DASHBOARD_"}


settings = Settings()
