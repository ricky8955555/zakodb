from typing import Callable

from zakodb.types import ZakoDbHashMethod

_HashFunc = Callable[[bytes], int]


def _not_implemented_hash_func(_: bytes) -> int:
    raise NotImplementedError


_hash_funcs: dict[ZakoDbHashMethod, _HashFunc] = {}

try:
    import xxhash

    _hash_funcs[ZakoDbHashMethod.XXH32] = xxhash.xxh32_intdigest
    _hash_funcs[ZakoDbHashMethod.XXH64] = xxhash.xxh64_intdigest
except ImportError:
    pass

try:
    import cityhash

    _hash_funcs[ZakoDbHashMethod.CITY32] = (
        cityhash.CityHash32
    )  # pyright: ignore[reportUnknownMemberType]
    _hash_funcs[ZakoDbHashMethod.CITY64] = (
        cityhash.CityHash64
    )  # pyright: ignore[reportUnknownMemberType]
except ImportError:
    pass


class Hasher:
    _method: ZakoDbHashMethod

    _hash: _HashFunc

    @property
    def method(self) -> ZakoDbHashMethod:
        return self._method

    @staticmethod
    def _get_hash_func(method: ZakoDbHashMethod) -> _HashFunc:
        try:
            return _hash_funcs[method]
        except KeyError:
            return _not_implemented_hash_func

    def __init__(self, method: ZakoDbHashMethod) -> None:
        self._method = method
        self._hash = self._get_hash_func(method)

    def hash(self, data: bytes) -> int:
        return self._hash(data)
