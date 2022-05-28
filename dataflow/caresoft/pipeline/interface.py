from typing import Callable, Any, Optional
from dataclasses import dataclass

from caresoft.repo import Data


@dataclass
class Pipeline:
    name: str
    params_fn: Callable[[dict[str, Any]], Any]
    get: Callable[[Any], Data]
    transform: Callable[[Data], Data]
    schema: list[dict[str, Any]]
    id_key: Optional[str] = None
    cursor_key: Optional[str] = None
    queue_task: bool = False
