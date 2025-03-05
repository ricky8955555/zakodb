from typing import Callable

from zakodb.types import ZakoDbHashMethod

_HashFunc = Callable[[bytes], int]


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


def zakodb_hash(data: bytes, method: ZakoDbHashMethod) -> int:
    try:
        func = _hash_funcs[method]
    except KeyError:
        raise NotImplementedError

    return func(data)    
