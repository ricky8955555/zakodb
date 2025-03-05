from typing import Any, TypeVar

_T = TypeVar("_T")


def check_type(value: Any, expected: type[_T]) -> _T:
    if not isinstance(value, expected):
        raise TypeError
    return value
