import os
import random
import time
import logging

logger = logging.getLogger(__name__)

_MIN_DELAY = int(os.environ.get("MIN_DELAY", 5))
_MAX_DELAY = int(os.environ.get("MAX_DELAY", 15))

RETRYABLE_SIGNALS = [
    "403", "forbidden", "rate limit", "ratelimit",
    "too many requests", "429", "timeout", "timed out",
    "connection", "network", "temporary", "503", "502", "500",
    "520", "521", "524", "reset by peer", "broken pipe",
    "eof occurred", "remote end closed", "ssl",
]

PAUSE_SIGNALS = [
    "ip blocked", "ip_blocked", "account suspended",
    "checkpoint required", "not available in your country",
]


def is_retryable(error_msg: str) -> bool:
    msg = error_msg.lower()
    return any(s in msg for s in RETRYABLE_SIGNALS)


def should_pause(error_msg: str) -> bool:
    msg = error_msg.lower()
    return any(s in msg for s in PAUSE_SIGNALS)


def backoff_delay(retry_count: int):
    total = min(_MIN_DELAY * (2 ** retry_count) + random.randint(_MIN_DELAY, _MAX_DELAY), 90)
    logger.info(f"Backoff delay: {total}s (retry #{retry_count + 1})")
    time.sleep(total)


def inter_download_delay():
    delay = random.randint(_MIN_DELAY, _MAX_DELAY)
    logger.info(f"Inter-download delay: {delay}s")
    time.sleep(delay)
