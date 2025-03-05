from zakodb.hasher import zakodb_hash
from zakodb.types import ZakoDbHashMethod, ZakoDbHashedBytes


def create_hashed_bytes(data: bytes, method: ZakoDbHashMethod) -> ZakoDbHashedBytes:
    hash = zakodb_hash(data, method)
    hashed_bytes = ZakoDbHashedBytes(hash=hash, content=data)
    return hashed_bytes
