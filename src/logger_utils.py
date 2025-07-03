import logging
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
