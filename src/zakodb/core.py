import io
import struct
from typing import IO, Any, Generator

from zakodb.const import (
    ZAKODB_BYTES_LENGTH_INTTYPE,
    ZAKODB_EXTRA_FIELD_COUNT_INTTYPE,
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
from zakodb.query import QueryExecutor
from zakodb.types import (
    ZakoDbEntry,
    ZakoDbFieldProperty,
    ZakoDbHashedBytes,
    ZakoDbHashMethod,
    ZakoDbMetadata,
    ZakoDbPythonType,
    ZakoDbType,
    ZakoDbTypedValue,
)


class _ZakoDbPrimitiveIO:
    io: IO[bytes]

    def __init__(self, io: IO[bytes]) -> None:
        self.io = io

    def read(self, n: int = -1) -> bytes:
        data = self.io.read(n)

        if len(data) == 0:
            raise EOFError

        return data

    def read_string(self) -> str:
        content = bytearray()
        while True:
            ch = self.io.read(1)
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
        self.io.write(data)

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


class _ZakoDbIO:
    io: _ZakoDbPrimitiveIO

    @property
    def underlying(self) -> IO[bytes]:
        return self.io.io

    def __init__(self, io: IO[bytes]) -> None:
        self.io = _ZakoDbPrimitiveIO(io)

    def expect_magic(self) -> None:
        try:
            read = self.io.read(len(ZAKODB_MAGIC))
        except EOFError:
            raise NotAZakoDbError

        if read != ZAKODB_MAGIC:
            raise NotAZakoDbError

    def write_magic(self) -> None:
        self.io.write(ZAKODB_MAGIC)

    def write_hashed_bytes(self, obj: ZakoDbHashedBytes) -> None:
        self.io.write_int(obj.hash, **ZAKODB_HASH_INTTYPE)
        self.io.write_bytes(obj.content)

    def read_hashed_bytes(self) -> ZakoDbHashedBytes:
        hash = self.io.read_int(**ZAKODB_HASH_INTTYPE)
        content = self.io.read_bytes()
        obj = ZakoDbHashedBytes(hash=hash, content=content)
        return obj

    def read_metadata(self) -> ZakoDbMetadata:
        version = self.io.read_int(**ZAKODB_VERSION_INTTYPE)

        if version != 1:
            raise NotImplementedError

        hash_method = ZakoDbHashMethod(self.io.read_int(**ZAKODB_HASH_METHOD_INTTYPE))
        field_count = self.io.read_int(**ZAKODB_FIELD_COUNT_INTTYPE)
        field_props: list[ZakoDbFieldProperty] = []

        for _ in range(field_count):
            field_name = self.io.read_string()
            field_type = ZakoDbType(self.io.read_int(**ZAKODB_TYPE_INTTYPE))
            field_prop = ZakoDbFieldProperty(name=field_name, type=field_type)
            field_props.append(field_prop)

        extra_field_count = self.io.read_int(**ZAKODB_EXTRA_FIELD_COUNT_INTTYPE)
        extra_fields: dict[str, ZakoDbTypedValue] = {}

        for _ in range(extra_field_count):
            extra_field_name = self.io.read_string()
            extra_field_type = ZakoDbType(self.io.read_int(**ZAKODB_TYPE_INTTYPE))
            extra_field_value = self.read_value(extra_field_type)
            typed_extra_field = ZakoDbTypedValue(type=extra_field_type, value=extra_field_value)
            extra_fields[extra_field_name] = typed_extra_field

        metadata = ZakoDbMetadata(
            version=version,
            hash_method=hash_method,
            field_props=tuple(field_props),
            extra_fields=extra_fields,
        )

        return metadata

    def write_metadata(self, metadata: ZakoDbMetadata) -> None:
        self.io.write_int(metadata.version, **ZAKODB_VERSION_INTTYPE)
        self.io.write_int(metadata.hash_method, **ZAKODB_HASH_METHOD_INTTYPE)

        field_count = len(metadata.field_props)
        self.io.write_int(field_count, **ZAKODB_FIELD_COUNT_INTTYPE)

        for field_prop in metadata.field_props:
            self.io.write_string(field_prop.name)
            self.io.write_int(field_prop.type, **ZAKODB_TYPE_INTTYPE)

        extra_field_count = len(metadata.extra_fields)
        self.io.write_int(extra_field_count, **ZAKODB_EXTRA_FIELD_COUNT_INTTYPE)

        for extra_field_name, extra_field_value in metadata.extra_fields.items():
            self.io.write_string(extra_field_name)
            self.write_typed_value(extra_field_value)

    @staticmethod
    def check_type(value: ZakoDbPythonType, typ: ZakoDbType) -> ZakoDbPythonType:
        if not isinstance(value, ZAKODB_PYTHON_TYPES[typ]):
            raise TypeError
        return value

    def write_type(self, typ: ZakoDbType) -> None:
        self.io.write_int(typ, **ZAKODB_TYPE_INTTYPE)

    def write_value(self, value: ZakoDbPythonType, typ: ZakoDbType) -> None:
        self.check_type(value, typ)
        self.write_value_unchecked(value, typ)

    def write_value_unchecked(self, value: Any, typ: ZakoDbType) -> None:
        if typ in ZAKODB_INT_INTTYPES:
            int_type = ZAKODB_INT_INTTYPES[typ]
            self.io.write_int(value, **int_type)
            return

        match typ:
            case ZakoDbType.FLOAT32:
                self.io.write_float32(value)
            case ZakoDbType.FLOAT64:
                self.io.write_float64(value)
            case ZakoDbType.BYTES:
                self.io.write_bytes(value)
            case ZakoDbType.HASHED_BYTES:
                self.write_hashed_bytes(value)
            case _:
                raise NotImplementedError

    def write_typed_value(self, value: ZakoDbTypedValue) -> None:
        self.check_type(value.value, value.type)
        self.write_type(value.type)
        self.write_value(value.value, value.type)

    def write_typed_value_unchecked(self, value: ZakoDbTypedValue) -> None:
        self.write_type(value.type)
        self.write_value(value.value, value.type)

    def read_value(self, typ: ZakoDbType) -> ZakoDbPythonType:
        if typ in ZAKODB_INT_INTTYPES:
            int_type = ZAKODB_INT_INTTYPES[typ]
            return self.io.read_int(**int_type)

        match typ:
            case ZakoDbType.FLOAT32:
                return self.io.read_float32()
            case ZakoDbType.FLOAT64:
                return self.io.read_float64()
            case ZakoDbType.BYTES:
                return self.io.read_bytes()
            case ZakoDbType.HASHED_BYTES:
                return self.read_hashed_bytes()
            case _:
                raise NotImplementedError

    def write_entry(
        self,
        entry: ZakoDbEntry,
        field_props: tuple[ZakoDbFieldProperty, ...],
    ) -> None:
        fields_with_types: list[tuple[ZakoDbPythonType, ZakoDbType]] = []

        for field_prop in field_props:
            fields_with_types.append(
                (
                    self.check_type(entry[field_prop.name], field_prop.type),
                    field_prop.type,
                )
            )

        for value, typ in fields_with_types:
            self.write_value_unchecked(value, typ)

    def read_entry(self, field_props: tuple[ZakoDbFieldProperty, ...]) -> ZakoDbEntry:
        fields: ZakoDbEntry = {}

        for field_prop in field_props:
            fields[field_prop.name] = self.read_value(field_prop.type)

        return fields


class ZakoDb:
    _io: _ZakoDbIO
    _metadata: ZakoDbMetadata
    _data_offset: int

    @property
    def io(self) -> IO[bytes]:
        return self._io.underlying

    @property
    def metadata(self) -> ZakoDbMetadata:
        return self._metadata

    def __init__(self, io: IO[bytes], metadata: ZakoDbMetadata, data_offset: int) -> None:
        self._io = _ZakoDbIO(io)
        self._metadata = metadata
        self._data_offset = data_offset

    @staticmethod
    def create(io: IO[bytes], metadata: ZakoDbMetadata) -> "ZakoDb":
        dbio = _ZakoDbIO(io)

        dbio.write_magic()
        dbio.write_metadata(metadata)

        data_offset = io.tell()

        return ZakoDb(io, metadata, data_offset)

    @staticmethod
    def load(io: IO[bytes]) -> "ZakoDb":
        dbio = _ZakoDbIO(io)

        dbio.expect_magic()
        metadata = dbio.read_metadata()

        data_offset = io.tell()

        return ZakoDb(io, metadata, data_offset)

    def append_entry(self, entry: ZakoDbEntry) -> None:
        self.io.seek(0, io.SEEK_END)
        self._io.write_entry(entry, self._metadata.field_props)

    def iter_entries(self) -> Generator[ZakoDbEntry, None, None]:
        self.io.seek(self._data_offset)

        while True:
            try:
                yield self._io.read_entry(self._metadata.field_props)
            except EOFError:
                return

    def find_entry(self, query: QueryExecutor) -> Generator[ZakoDbEntry, None, None]:
        return (entry for entry in self.iter_entries() if query(entry))
