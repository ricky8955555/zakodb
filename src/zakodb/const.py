from zakodb.types import ZakoDbHashedBytes, ZakoDbPythonType, ZakoDbType

ZAKODB_MAGIC = b"z4k0"

ZAKODB_PYTHON_TYPES: dict[ZakoDbType, type[ZakoDbPythonType]] = {
    ZakoDbType.INT8: int,
    ZakoDbType.UINT8: int,
    ZakoDbType.INT16: int,
    ZakoDbType.UINT16: int,
    ZakoDbType.INT32: int,
    ZakoDbType.UINT32: int,
    ZakoDbType.INT64: int,
    ZakoDbType.UINT64: int,
    ZakoDbType.BYTES: bytes,
    ZakoDbType.HASHED_BYTES: ZakoDbHashedBytes,
    ZakoDbType.FLOAT32: float,
    ZakoDbType.FLOAT64: float,
}
