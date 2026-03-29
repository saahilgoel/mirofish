"""
Zep API rate limit handler.
Wraps the Zep client to automatically retry on 429 rate limit errors,
respecting the retry-after header from Zep's free plan (5 req/min).
"""

import time
import functools
from zep_cloud.client import Zep
from zep_cloud.core.api_error import ApiError
from .logger import get_logger

logger = get_logger('mirofish.zep_rate_limit')

# Global request pacer: ensures minimum gap between Zep API calls
_last_request_time = 0.0
_MIN_REQUEST_GAP = 12.0  # 5 req/min = 1 req per 12s


def _pace_request():
    """Enforce minimum gap between Zep API calls to stay within free tier limits."""
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < _MIN_REQUEST_GAP:
        wait = _MIN_REQUEST_GAP - elapsed
        logger.debug(f"Pacing Zep request: waiting {wait:.1f}s")
        time.sleep(wait)
    _last_request_time = time.time()


def zep_retry(func):
    """Decorator that retries Zep API calls on 429 rate limit errors."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 10
        for attempt in range(max_retries + 1):
            _pace_request()
            try:
                return func(*args, **kwargs)
            except ApiError as e:
                if e.status_code == 429:
                    retry_after = 65  # default
                    if e.headers and 'retry-after' in e.headers:
                        try:
                            retry_after = int(e.headers['retry-after']) + 5
                        except (ValueError, TypeError):
                            pass
                    if attempt < max_retries:
                        logger.warning(
                            f"Zep rate limit hit (attempt {attempt + 1}/{max_retries}), "
                            f"waiting {retry_after}s..."
                        )
                        time.sleep(retry_after)
                        continue
                raise
            except Exception:
                raise
    return wrapper


class RateLimitedZep:
    """
    Proxy around the Zep client that auto-retries on 429 errors.
    Use this instead of Zep() directly.
    """

    def __init__(self, api_key: str):
        self._client = Zep(api_key=api_key)

    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if hasattr(attr, '__call__'):
            return zep_retry(attr)
        # For sub-objects like client.graph, wrap them too
        if hasattr(attr, '__class__') and not isinstance(attr, (str, int, float, bool)):
            return _SubProxy(attr)
        return attr


class _SubProxy:
    """Proxy for sub-objects of the Zep client (e.g., client.graph)."""

    def __init__(self, obj):
        self._obj = obj

    def __getattr__(self, name):
        attr = getattr(self._obj, name)
        if callable(attr):
            return zep_retry(attr)
        if hasattr(attr, '__class__') and not isinstance(attr, (str, int, float, bool)):
            return _SubProxy(attr)
        return attr
