from __future__ import annotations

from typing import Callable, Any, Optional, Iterable
from dataclasses import dataclass


@dataclass
class Key:
    id: str
    partition: str


@dataclass
class Pipeline:
    name: str
    params_fn: Callable[[dict[str, Any]], Any]
    get: Callable[[Any], Iterable[Any]]
    transform: Callable[[dict[str, Any]], dict[str, Any]]
    schema: list[dict[str, Any]]
    key: Optional[Key] = None
    details: Optional[Pipeline] = None
