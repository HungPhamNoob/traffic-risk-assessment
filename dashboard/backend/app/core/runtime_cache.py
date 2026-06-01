"""Small in-process TTL cache for hot dashboard reads."""

from __future__ import annotations

import copy
import time

from dataclasses import dataclass
from threading import Lock
from typing import Callable, TypeVar

T = TypeVar("T")


@dataclass
class _CacheEntry:
    value: object
    expires_at: float


_CACHE: dict[tuple[str, tuple[object, ...]], _CacheEntry] = {}
_CACHE_LOCK = Lock()
_MAX_ENTRIES = 256


def _normalize_key_part(value: object) -> object:
    if isinstance(value, list):
        return tuple(_normalize_key_part(item) for item in value)
    if isinstance(value, dict):
        return tuple(
            sorted((str(key), _normalize_key_part(item)) for key, item in value.items())
        )
    return value


def cached_result(
    namespace: str,
    key_parts: tuple[object, ...],
    ttl_seconds: float,
    loader: Callable[[], T],
) -> T:
    """Return a cached value when fresh, otherwise compute and store it."""
    cache_key = (namespace, tuple(_normalize_key_part(part) for part in key_parts))
    now = time.monotonic()

    with _CACHE_LOCK:
        entry = _CACHE.get(cache_key)
        if entry and entry.expires_at > now:
            return copy.deepcopy(entry.value)  # type: ignore[return-value]

    value = loader()
    expires_at = now + max(ttl_seconds, 0)

    with _CACHE_LOCK:
        if len(_CACHE) >= _MAX_ENTRIES:
            expired_keys = [
                key for key, entry in _CACHE.items() if entry.expires_at <= now
            ]
            for expired_key in expired_keys:
                _CACHE.pop(expired_key, None)

            if len(_CACHE) >= _MAX_ENTRIES and _CACHE:
                oldest_key = min(_CACHE.items(), key=lambda item: item[1].expires_at)[0]
                _CACHE.pop(oldest_key, None)

        _CACHE[cache_key] = _CacheEntry(
            value=copy.deepcopy(value),
            expires_at=expires_at,
        )

    return copy.deepcopy(value)
