import io
import struct
from typing import IO, Any, Generator

from zakodb._utils import check_type
from zakodb.const import (
    ZAKODB_BYTES_LENGTH_INTTYPE,
    ZAKODB_FIELD_COUNT_INTTYPE,
    ZAKODB_HASH_INTTYPE,
    ZAKODB_HASH_METHOD_INTTYPE,
    ZAKODB_INT_INTTYPES,
    ZAKODB_MAGIC,
    ZAKODB_PYTHON_TYPES,
    ZAKODB_TYPE_INTTYPE,
    ZAKODB_VERSION_INTTYPE,
)
from zakodb.exc import NotAZakoDbError
from zakodb.hasher import Hasher
from zakodb.query import QueryExecutor
from zakodb.types import (
    ZakoDbFieldProperty,
    ZakoDbHashedBytes,
    ZakoDbHashMethod,
    ZakoDbMetadata,
    ZakoDbPythonType,
    ZakoDbType,
)


class _ZakoDbPrimitiveIO:
    _io: IO[bytes]

    @property
    def underlying(self) -> IO[bytes]:
        return self._io

    def __init__(self, io: IO[bytes]) -> None:
        self._io = io

    def read(self, n: int = -1) -> bytes:
        data = self._io.read(n)

        if len(data) == 0:
            raise EOFError

        return data

    def read_string(self) -> str:
        content = bytearray()
        while True:
            ch = self._io.read(1)
            if ch == b"\0":
                break
            content.extend(ch)
        return content.decode("utf-8")

    def read_bytes(self) -> bytes:
        length = self.read_int(**ZAKODB_BYTES_LENGTH_INTTYPE)
        content = self.read(length)
        return content

    def read_int(self, length: int, signed: bool) -> int:
        data = self.read(length)
        number = int.from_bytes(data, "big", signed=signed)
        return number

    def read_float32(self) -> float:
        data = self.read(4)
        number, *_ = struct.unpack(">f", data)
        return number

    def read_float64(self) -> float:
        data = self.read(8)
        number, *_ = struct.unpack(">d", data)
        return number

    def write_string(self, string: str) -> None:
        encoded = string.encode("utf-8")
        self.write(encoded + b"\0")

    def write(self, data: bytes) -> None:
        self._io.write(data)

    def write_bytes(self, content: bytes) -> None:
        length = len(content)
        self.write_int(length, **ZAKODB_BYTES_LENGTH_INTTYPE)
        self.write(content)

    def write_int(self, number: int, length: int, signed: bool) -> None:
        assert length in [1, 2, 4, 8], "invalid length for integer."
        data = number.to_bytes(length, "big", signed=signed)
        self.write(data)

    def write_float32(self, number: float) -> None:
        data = struct.pack(">f", number)
        self.write(data)

    def write_float64(self, number: float) -> None:
        data = struct.pack(">d", number)
        self.write(data)


class ZakoDb:
    _io: _ZakoDbPrimitiveIO
    _metadata: ZakoDbMetadata
    _hasher: Hasher
    _data_offset: int

    @property
    def io(self) -> IO[bytes]:
        return self._io.underlying

    @property
    def metadata(self) -> ZakoDbMetadata:
        return self._metadata

    @property
    def hasher(self) -> Hasher:
        return self._hasher

    def __init__(self, io: IO[bytes]) -> None:
        self._io = _ZakoDbPrimitiveIO(io)

        self._expect_magic()
        self._metadata = self._read_metadata()

        self._data_offset = self.io.tell()
        self._hasher = Hasher(self._metadata.hash_method)

    def _expect_magic(self) -> None:
        read = self._io.read(len(ZAKODB_MAGIC))

        if read != ZAKODB_MAGIC:
            raise NotAZakoDbError

    def _read_metadata(self) -> ZakoDbMetadata:
        version = self._io.read_int(**ZAKODB_VERSION_INTTYPE)

        if version != 1:
            raise NotImplementedError

        hash_method = ZakoDbHashMethod(self._io.read_int(**ZAKODB_HASH_METHOD_INTTYPE))
        field_count = self._io.read_int(**ZAKODB_FIELD_COUNT_INTTYPE)
        field_props: list[ZakoDbFieldProperty] = []

        for _ in range(field_count):
            field_name = self._io.read_string()
            field_type = ZakoDbType(self._io.read_int(**ZAKODB_TYPE_INTTYPE))
            field_prop = ZakoDbFieldProperty(name=field_name, type=field_type)
            field_props.append(field_prop)

        metadata = ZakoDbMetadata(
            version=version,
            hash_method=hash_method,
            field_props=tuple(field_props),
        )

        return metadata

    def create_hashed_bytes(self, content: bytes) -> ZakoDbHashedBytes:
        hash = self._hasher.hash(content)
        obj = ZakoDbHashedBytes(hash=hash, content=content)
        return obj

    def _write_hashed_bytes(self, obj: ZakoDbHashedBytes) -> None:
        self._io.write_int(obj.hash, **ZAKODB_HASH_INTTYPE)
        self._io.write_bytes(obj.content)

    def _read_hashed_bytes(self) -> ZakoDbHashedBytes:
        hash = self._io.read_int(**ZAKODB_HASH_INTTYPE)
        content = self._io.read_bytes()
        obj = ZakoDbHashedBytes(hash=hash, content=content)
        return obj

    def _write_value_unchecked(self, value: Any, typ: ZakoDbType) -> None:
        if typ in ZAKODB_INT_INTTYPES:
            int_type = ZAKODB_INT_INTTYPES[typ]
            self._io.write_int(value, **int_type)
            return

        match typ:
            case ZakoDbType.FLOAT32:
                self._io.write_float32(value)
            case ZakoDbType.FLOAT64:
                self._io.write_float64(value)
            case ZakoDbType.BYTES:
                self._io.write_bytes(value)
            case ZakoDbType.HASHED_BYTES:
                self._write_hashed_bytes(value)
            case _:
                raise NotImplementedError

    def _read_value(self, typ: ZakoDbType) -> ZakoDbPythonType:
        if typ in ZAKODB_INT_INTTYPES:
            int_type = ZAKODB_INT_INTTYPES[typ]
            return self._io.read_int(**int_type)

        match typ:
            case ZakoDbType.FLOAT32:
                return self._io.read_float32()
            case ZakoDbType.FLOAT64:
                return self._io.read_float64()
            case ZakoDbType.BYTES:
                return self._io.read_bytes()
            case ZakoDbType.HASHED_BYTES:
                return self._read_hashed_bytes()
            case _:
                raise NotImplementedError

    @staticmethod
    def _check_type(value: ZakoDbPythonType, typ: ZakoDbType) -> ZakoDbPythonType:
        return check_type(value, ZAKODB_PYTHON_TYPES[typ])

    def append_entry(self, fields: dict[str, ZakoDbPythonType]) -> None:
        fields_with_types: list[tuple[ZakoDbPythonType, ZakoDbType]] = []

        for field_prop in self._metadata.field_props:
            fields_with_types.append(
                (
                    self._check_type(fields[field_prop.name], field_prop.type),
                    field_prop.type,
                )
            )

        self.io.seek(0, io.SEEK_END)

        for value, typ in fields_with_types:
            self._write_value_unchecked(value, typ)

    def _read_entry(self) -> dict[str, ZakoDbPythonType]:
        fields: dict[str, ZakoDbPythonType] = {}

        for field_prop in self._metadata.field_props:
            fields[field_prop.name] = self._read_value(field_prop.type)

        return fields

    def iter_entries(self) -> Generator[dict[str, ZakoDbPythonType], None, None]:
        self.io.seek(self._data_offset)

        while True:
            try:
                yield self._read_entry()
            except EOFError:
                return

    def find_entry(
        self, query: QueryExecutor
    ) -> Generator[dict[str, ZakoDbPythonType], None, None]:
        return (entry for entry in self.iter_entries() if query(entry))


def create_zako_db(underlying_io: IO[bytes], metadata: ZakoDbMetadata) -> None:
    io = _ZakoDbPrimitiveIO(underlying_io)

    io.write(ZAKODB_MAGIC)

    io.write_int(metadata.version, **ZAKODB_VERSION_INTTYPE)
    io.write_int(metadata.hash_method, **ZAKODB_HASH_METHOD_INTTYPE)

    field_count = len(metadata.field_props)
    io.write_int(field_count, **ZAKODB_FIELD_COUNT_INTTYPE)

    for field_prop in metadata.field_props:
        io.write_string(field_prop.name)
        io.write_int(field_prop.type, **ZAKODB_TYPE_INTTYPE)

    underlying_io.seek(0)
