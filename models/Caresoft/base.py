from typing import Callable, TypedDict

from libs.caresoft import Getter


class CaresoftResource(TypedDict):
    name: str
    get: Getter
    transform: Callable[[list[dict]], list[dict]]
    schema: list[dict]
