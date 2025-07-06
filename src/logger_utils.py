import logging
from decimal import Decimal
from pythonjsonlogger import jsonlogger

# Configure the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clear any existing handlers (important for ECS reuse)
if logger.hasHandlers():
    logger.handlers.clear()

# Stream handler to stdout
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# JSON formatter for structured logs
formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# In-memory log event list
log_events = []

def log_and_store(event, level="info"):
    """
    Log event both to CloudWatch and to the in-memory list for later upload.
    """
    log_events.append(event)
    getattr(logger, level)(event)


def safe_decimal(value):
    if value is None:
        return None
    if isinstance(value, float):
        return Decimal(str(round(value, 4)))
    return value

def safe_dynamodb_value(value):
    from decimal import Decimal
    import datetime
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, datetime.date):
        return value.isoformat()  # e.g., "2025-03-30"
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    return value

