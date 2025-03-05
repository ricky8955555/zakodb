from dataclasses import dataclass
from enum import IntEnum
from typing import TypedDict


class IntType(TypedDict):
    length: int
    signed: bool


@dataclass(frozen=True, kw_only=True)
class ZakoDbHashedBytes:
    hash: int
    content: bytes

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ZakoDbHashedBytes):
            return other.hash == self.hash and other.content == self.content
        if isinstance(other, bytes):
            return other == self.content
        return False

    def __contains__(self, obj: object) -> bool:
        if isinstance(obj, ZakoDbHashedBytes):
            return obj.content in self.content
        else:
            return obj in self.content  # pyright: ignore[reportOperatorIssue]


class ZakoDbHashMethod(IntEnum):
    NONE = 0
    XXH32 = 1
    XXH64 = 2
    CITY32 = 3
    CITY64 = 4


class ZakoDbType(IntEnum):
    INT8 = 0
    UINT8 = 1
    INT16 = 2
    UINT16 = 3
    INT32 = 4
    UINT32 = 5
    INT64 = 6
    UINT64 = 7
    BYTES = 8
    HASHED_BYTES = 9
    FLOAT32 = 10
    FLOAT64 = 11


ZakoDbPythonType = int | float | bytes | ZakoDbHashedBytes


@dataclass(frozen=True, kw_only=True)
class ZakoDbFieldProperty:
    name: str
    type: ZakoDbType


@dataclass(frozen=True, kw_only=True)
class ZakoDbMetadata:
    version: int = 1
    hash_method: ZakoDbHashMethod
    field_props: tuple[ZakoDbFieldProperty, ...]
