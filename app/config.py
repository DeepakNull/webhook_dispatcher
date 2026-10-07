import os

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://dispatcher:dispatcher@localhost:5433/dispatcher"
)
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")

# Total delivery attempts before a webhook is sent to the DLQ
MAX_ATTEMPTS = 5
# Strict per-request timeout used by the worker (seconds)
HTTP_TIMEOUT = 5.0

#Sweeper: re-publish deliveries stuck in PENDING (API crashed between DB commit and publish)
SWEEP_INTERVAL = float(os.getenv("SWEEP_INTERVAL", "15"))
PENDING_STALE_SECONDS = float(os.getenv("PENDING_STALE_SECONDS", "60"))
  
  