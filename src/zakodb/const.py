from zakodb.types import IntType, ZakoDbHashedBytes, ZakoDbPythonType, ZakoDbType

ZAKODB_MAGIC = b"z4k0"

ZAKODB_VERSION_INTTYPE = IntType(length=1, signed=False)
ZAKODB_BYTES_LENGTH_INTTYPE = IntType(length=2, signed=False)
ZAKODB_FIELD_COUNT_INTTYPE = IntType(length=1, signed=False)
ZAKODB_EXTRA_FIELD_COUNT_INTTYPE = IntType(length=1, signed=False)
ZAKODB_TYPE_INTTYPE = IntType(length=1, signed=False)
ZAKODB_HASH_METHOD_INTTYPE = IntType(length=1, signed=False)
ZAKODB_HASH_INTTYPE = IntType(length=8, signed=False)

ZAKODB_INT_INTTYPES = {
    ZakoDbType.INT8: IntType(length=1, signed=True),
    ZakoDbType.UINT8: IntType(length=1, signed=False),
    ZakoDbType.INT16: IntType(length=2, signed=True),
    ZakoDbType.UINT16: IntType(length=2, signed=False),
    ZakoDbType.INT32: IntType(length=4, signed=True),
    ZakoDbType.UINT32: IntType(length=4, signed=False),
    ZakoDbType.INT64: IntType(length=8, signed=True),
    ZakoDbType.UINT64: IntType(length=8, signed=False),
}

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
