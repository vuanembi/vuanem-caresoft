from typing import Callable, Any, Optional, Protocol
from dataclasses import dataclass

from caresoft.repo import Data


class ParamsFn(Protocol):
    def __call__(self, *args) -> Any:
        pass


@dataclass
class Pipeline:
    name: str
    params_fn: ParamsFn
    get: Callable[[Any], Data]
    transform: Callable[[Data], Data]
    schema: list[dict[str, Any]]
    id_key: Optional[str] = None
    cursor_key: Optional[str] = None
    partition_key: Optional[str] = None
    queue_task: bool = False
